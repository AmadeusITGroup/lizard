# analytics/advanced_anomaly.py

from __future__ import annotations
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Optional
from sklearn.ensemble import IsolationForest
from collections import deque

EARTH_R_KM = 6371.0088


def _to_dt_utc(s):
    return pd.to_datetime(s, utc=True, errors="coerce")


def _haversine_km(lat1, lon1, lat2, lon2):
    """Calculate great-circle distance between two points in km."""
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))  # Clip to avoid numerical issues
    return EARTH_R_KM * c


def _neglog_rarity(series: pd.Series, smooth: float = 1.0) -> pd.Series:
    """Calculate negative log probability (rarity) for categorical values."""
    if series.empty:
        return pd.Series(np.zeros(len(series)), index=series.index)
    counts = series.value_counts(dropna=False)
    total = float(len(series))
    probs = series.map(lambda x: (counts.get(x, 0.0) + smooth) / (total + smooth))
    return (-np.log(probs.clip(lower=1e-9))).astype(float)


def _user_home(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate median location (home) for each user."""
    d = df.dropna(subset=["geo_lat", "geo_lon"]).copy()
    if d.empty:
        return pd.DataFrame(columns=["user_id", "home_lat", "home_lon"])
    g = (
        d.groupby("user_id")[["geo_lat", "geo_lon"]]
        .median()
        .rename(columns={"geo_lat": "home_lat", "geo_lon": "home_lon"})
        .reset_index()
    )
    return g


def compute_advanced_anomaly(
        df: pd.DataFrame,
        contamination: float = 0.05,
        speed_kmh_thr: float = 900.0,
dist_km_thr: float = 2000.0,
event_velocity_thr

: float = 10.0,  # NEW: events/min threshold
rare_hour_prob: float = 0.05,
random_state: int = 42,
) -> pd.DataFrame:
    """
    Compute advanced anomaly detection using Isolation Forest with multiple features.

    Features used:
    - Geographic:  distance from previous location, distance from home, travel speed
    - Temporal: hour rarity, cyclical hour encoding, event velocity (NEW)
    - Novelty: new device/IP flags (first-time and rolling window)
    - Behavioral: failure rate z-score

    Returns DataFrame with added columns:
    - anom_score (0-1): anomaly score
    - anomaly (bool): anomaly flag
    - reasons (list): list of reason dictionaries
    - explain (str): human-readable explanation
    """
    if df.empty:
        out = df.copy()
        out["anom_score"] = 0.0
        out["anomaly"] = False
        out["reasons"] = [[] for _ in range(len(out))]
        out["explain"] = ""
        return out

    d = df.copy()
    d["ts"] = _to_dt_utc(d["ts"])
    d = d.sort_values(["user_id", "ts"], kind="mergesort")

    # Ensure user_id exists and is safe for grouping
    if "user_id" not in d.columns:
        d["user_id"] = "unknown"
    d["user_id"] = d["user_id"].astype("string").fillna("unknown")

    # Ensure required columns exist
    for col in ("geo_lat", "geo_lon", "ip", "device_id", "event_type"):
        if col not in d.columns:
            d[col] = np.nan if col in ("geo_lat", "geo_lon") else ""

    # ===================================================================
    # GEO FEATURES:  Geographic speed and distances
    # ===================================================================
    d["lat_prev"] = d.groupby("user_id")["geo_lat"].shift(1)
    d["lon_prev"] = d.groupby("user_id")["geo_lon"].shift(1)
    d["ts_prev"] = d.groupby("user_id")["ts"].shift(1)

    with np.errstate(invalid="ignore", divide="ignore"):
        d["dist_prev_km"] = _haversine_km(
            d["geo_lat"], d["geo_lon"], d["lat_prev"], d["lon_prev"]
        )
        dt_hours = (d["ts"] - d["ts_prev"]).dt.total_seconds() / 3600.0
        d["speed_kmh"] = d["dist_prev_km"] / dt_hours.replace(0, np.nan)

    # Home location and distance from home
    home = _user_home(d)
    d = d.merge(home, on="user_id", how="left")
    d["dist_home_km"] = _haversine_km(
        d["geo_lat"], d["geo_lon"], d["home_lat"], d["home_lon"]
    )

    # ===================================================================
    # TIME FEATURES: Hour rarity and cyclical encoding
    # ===================================================================
    d["hour"] = d["ts"].dt.hour.fillna(0).astype(int)
    h_counts = d.groupby(["user_id", "hour"]).size().rename("hcount").reset_index()
    h_totals = (
        h_counts.groupby("user_id")["hcount"].sum().rename("htotal").reset_index()
    )
    h_probs = h_counts.merge(h_totals, on="user_id")
    h_probs["hour_prob"] = (h_probs["hcount"] / h_probs["htotal"].clip(lower=1)).astype(
        float
    )
    d = d.merge(
        h_probs[["user_id", "hour", "hour_prob"]],
        on=["user_id", "hour"],
        how="left",
    )
    d["hour_prob"] = d["hour_prob"].fillna(0.0)
    d["hour_rarity"] = (-np.log(d["hour_prob"].clip(lower=1e-6))).astype(float)

    # Cyclical hour encoding
    theta = 2 * np.pi * (d["hour"].fillna(0) / 24.0)
    d["h_sin"] = np.sin(theta)
    d["h_cos"] = np.cos(theta)

    # ===================================================================
    # EVENT VELOCITY FEATURES (NEW)
    # ===================================================================
    # Calculate rolling event count in 5-minute windows per user
    d["event_velocity"] = (
        d.groupby("user_id", group_keys=False)
        .apply(lambda g: g.set_index("ts").rolling("5min", min_periods=1).count().iloc[:, 0] / 5.0)
        .reset_index(level=0, drop=True)
    )

    # Calculate per-user event velocity statistics for z-score
    user_velocity_stats = d.groupby("user_id")["event_velocity"].agg(
        event_velocity_mean="mean",
        event_velocity_std="std"
    ).reset_index()

    d = d.merge(user_velocity_stats, on="user_id", how="left")

    # Event velocity z-score (how abnormal is this rate for this user?)
    d["event_velocity_std"] = d["event_velocity_std"].replace(0, 1.0).fillna(1.0)
    d["event_velocity_z"] = (
            (d["event_velocity"] - d["event_velocity_mean"]) /
            (d["event_velocity_std"] + 1e-6)
    )

    # ===================================================================
    # NOVELTY FEATURES: First-time and rolling window detection
    # ===================================================================
    d["device_id"] = d["device_id"].astype("string").fillna("unknown")
    d["ip"] = d["ip"].astype("string").fillna("unknown")


    def _first_time_flag(g: pd.Series) -> pd.Series:
        """Flag first occurrence of each value within user group."""
        seen: set = set()
        out = []
        for v in g.tolist():
            out.append(1 if v not in seen else 0)
            seen.add(v)
        return pd.Series(out, index=g.index, dtype="int8")


    def _rolling_novelty_flag(g: pd.Series, window: int = 30) -> pd.Series:
        """Flag values not seen in last N events within user group."""
        q: deque = deque(maxlen=window)
        out = []
        for v in g.tolist():
            out.append(1 if v not in q else 0)
            q.append(v)
        return pd.Series(out, index=g.index, dtype="int8")


    d["is_new_device"] = (
        d.groupby("user_id", sort=False)["device_id"]
        .apply(_first_time_flag)
        .reset_index(level=0, drop=True)
    )
    d["is_new_ip"] = (
        d.groupby("user_id", sort=False)["ip"]
        .apply(_first_time_flag)
        .reset_index(level=0, drop=True)
    )
    d["is_new_device_rolling"] = (
        d.groupby("user_id", sort=False)["device_id"]
        .apply(lambda s: _rolling_novelty_flag(s, window=30))
        .reset_index(level=0, drop=True)
    )
    d["is_new_ip_rolling"] = (
        d.groupby("user_id", sort=False)["ip"]
        .apply(lambda s: _rolling_novelty_flag(s, window=30))
        .reset_index(level=0, drop=True)
    )

    # Global rarity of device/IP
    d["device_rarity_global"] = _neglog_rarity(d["device_id"].astype(str).fillna(""))
    d["ip_rarity_global"] = _neglog_rarity(d["ip"].astype(str).fillna(""))

    # ===================================================================
    # FAILURE SPIKE PROXY: Z-score of failure rate
    # ===================================================================
    d["event_type"] = d["event_type"].fillna("").astype(str)
    d["is_fail"] = d["event_type"].str.contains("fail", case=False, na=False).astype(int)
    d["minute"] = d["ts"].dt.floor("min")

    per_min = (
        d.groupby(["user_id", "minute"])["is_fail"]
        .sum()
        .rename("fail_count")
        .reset_index()
    )
    per_min["mu"] = per_min.groupby("user_id")["fail_count"].transform(
        lambda s: s.ewm(alpha=0.2, adjust=False).mean()
    )
    mad = per_min.groupby("user_id")["fail_count"].transform(
        lambda s: (np.abs(s - s.median())).rolling(1440, min_periods=30).median()
    )
    non_zero_median = mad[mad > 0].median(skipna=True)
    if pd.isna(non_zero_median) or non_zero_median == 0:
        non_zero_median = 1.0
    mad = mad.replace(0, non_zero_median).fillna(1.0)
    per_min["z_fail"] = (per_min["fail_count"] - per_min["mu"]) / (
            1.4826 * (mad + 1e-6)
    )
    d = d.merge(
        per_min[["user_id", "minute", "z_fail"]],
        on=["user_id", "minute"],
        how="left",
    )
    d["z_fail"] = d["z_fail"].fillna(0.0)

    # ===================================================================
    # FEATURE MATRIX FOR ISOLATION FOREST
    # ===================================================================
    feat_cols = [
        "dist_prev_km",
        "dist_home_km",
        "speed_kmh",
        "event_velocity",  # NEW:  Raw event rate (events/min)
        "event_velocity_z",  # NEW: Normalized event rate
        "is_new_device",
        "is_new_ip",
        "is_new_device_rolling",
        "is_new_ip_rolling",
        "device_rarity_global",
        "ip_rarity_global",
        "hour_rarity",
        "h_sin",
        "h_cos",
        "z_fail",
    ]
    X = d[feat_cols].copy()
    X = X.replace([np.inf, -np.inf], np.nan)

    # Fill NaN with median (or 0 if median is not finite)
    for col in X.columns:
        med = X[col].median()
        fill_val = med if np.isfinite(med) else 0.0
        X[col] = X[col].fillna(fill_val)

    # Check if we have enough samples for Isolation Forest
    if len(X) < 10:
        d["anom_score"] = 0.0
        d["anomaly"] = False
        d["reasons"] = [[] for _ in range(len(d))]
        d["explain"] = ""
        return d

    # Fit Isolation Forest
    clf = IsolationForest(
        n_estimators=200,
        contamination=float(contamination),
        random_state=random_state,
        n_jobs=-1,
    )
    clf.fit(X)

    # Get anomaly scores (higher = more anomalous)
    raw = -clf.score_samples(X)
    p2, p98 = np.percentile(raw, 2), np.percentile(raw, 98)
    denom = (p98 - p2) if (p98 - p2) > 1e-9 else (raw.max() - raw.min() + 1e-9)
    anom_score = np.clip((raw - p2) / denom, 0.0, 1.0)
    anomaly = clf.predict(X) == -1

    d["anom_score"] = anom_score
    d["anomaly"] = anomaly


    # ===================================================================
    # EXPLAINABILITY REASONS
    # ===================================================================
    def mk_reasons(row) -> List[Dict[str, Any]]:
        rs: List[Dict[str, Any]] = []

        if row["anomaly"]:
            rs.append(
                {
                    "code": "IF_OUTLIER",
                    "value": float(row["anom_score"]),
                    "thr": float(contamination),
                    "desc": "IsolationForest outlier",
                }
            )

        # Check geographic anomalies
        if np.isfinite(row.get("dist_home_km", np.nan)) and row["dist_home_km"] > dist_km_thr:
            rs.append({
                "code": "FAR_FROM_HOME",
                "value": float(row["dist_home_km"]),
                "thr": dist_km_thr,
                "desc": f"Distance from home {row['dist_home_km']:.0f}km exceeds {dist_km_thr}km",
            })

        if np.isfinite(row.get("dist_prev_km", np.nan)) and np.isfinite(row.get("speed_kmh", np.nan)):
            if (row["dist_prev_km"] > dist_km_thr) or (row["speed_kmh"] > speed_kmh_thr):
                rs.append({
                    "code": "GEO_JUMP",
                    "value": float(row["dist_prev_km"]),
                    "thr": dist_km_thr,
                    "desc": f"Geographic jump {row['dist_prev_km']:.0f}km at {row.get('speed_kmh', 0):.0f}km/h",
                })

        # NEW: Check for abnormally high event velocity
        if np.isfinite(row.get("event_velocity_z", np.nan)) and row["event_velocity_z"] > 3.0:
            rs.append({
                "code": "HIGH_EVENT_VELOCITY",
                "value": float(row["event_velocity"]),
                "thr": float(row.get("event_velocity_mean", 0) + 3 * row.get("event_velocity_std", 1)),
                "desc": f"Event rate {row['event_velocity']:.1f} events/min abnormally high (typical: {row.get('event_velocity_mean', 0):.1f})",
            })

        # Check novelty flags
        if row.get("is_new_device", 0) == 1 or row.get("is_new_device_rolling", 0) == 1:
            rs.append({
                "code": "NEW_DEVICE",
                "desc": "First or rarely seen device for this user",
            })

        if row.get("is_new_ip", 0) == 1 or row.get("is_new_ip_rolling", 0) == 1:
            rs.append({
                "code": "NEW_IP",
                "desc": "First or rarely seen IP for this user",
            })

        return rs


    d["reasons"] = d.apply(mk_reasons, axis=1)
    d["explain"] = d["reasons"].apply(
        lambda rs: "; ".join([x.get("desc", x.get("code", "")) for x in rs])
    )

    # Clean up temporary columns
    temp_cols = [
        "lat_prev", "lon_prev", "ts_prev", "home_lat", "home_lon",
        "minute", "event_velocity_mean", "event_velocity_std"
    ]
    d = d.drop(columns=[c for c in temp_cols if c in d.columns], errors="ignore")

    return d