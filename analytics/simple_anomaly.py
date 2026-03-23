# path: analytics/simple_anomaly.py
from __future__ import annotations
import numpy as np
import pandas as pd
from typing import List, Dict, Any
import re

# Bucket to pandas frequency mapping
_BUCKET_TO_FREQ = {
    "30s": "30S",
    "1m": "1min",
    "2m": "2min",
    "5m": "5min",
    "10m": "10min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1H",
    "3h": "3H",
    "6h": "6H",
    "12h": "12H",
    "1d": "1D",
}

# Bucket to minutes mapping (for window scaling)
_BUCKET_TO_MINUTES = {
    "30s": 0.5,
    "1m": 1,
    "2m": 2,
    "5m": 5,
    "10m": 10,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "3h": 180,
    "6h": 360,
    "12h": 720,
    "1d": 1440,
}

# Pandas 2.2+ requires lowercase frequency aliases
_DEPRECATED_FREQ = {"H": "h", "T": "min", "S": "s", "L": "ms", "U": "us", "N": "ns"}
_FREQ_RE = re.compile(r"(\d*)\s*([A-Za-z]+)")


def _normalize_freq(freq: str) -> str:
    """Convert deprecated uppercase pandas frequency aliases to lowercase."""
    m = _FREQ_RE.fullmatch(freq.strip())
    if m:
        num, alias = m.group(1), m.group(2)
        alias = _DEPRECATED_FREQ.get(alias, alias)
        return f"{num}{alias}"
    return freq


def _robust_minute_counts(
        df: pd.DataFrame,
        bucket: str = "1m",
        alpha: float = 0.2,
        window: int = 1440
) -> pd.DataFrame:
    """
    Per-user failures per bucket with EWMA baseline and MAD scale.

    Args:
        df: Input dataframe with ts, user_id, event_type columns
        bucket: Time bucket size ('1m', '5m', '15m', '1h', etc.)
        alpha: EWMA smoothing factor (lower = slower adaptation)
        window: Rolling window size in minutes for MAD calculation

    Returns:  user_id, bucket_time, count, mu, mad, zrobust, anom_score
    """
    d = df.copy()
    d["ts"] = pd.to_datetime(d["ts"], utc=True, errors="coerce")

    # Ensure user_id exists
    if "user_id" not in d.columns:
        d["user_id"] = "unknown"
    d["user_id"] = d["user_id"].fillna("unknown").astype(str)

    # Ensure event_type exists
    if "event_type" not in d.columns:
        d["event_type"] = "unknown"

    d["is_fail"] = (
        d["event_type"]
        .astype(str)
        .str.contains("fail", case=False, na=False)
        .astype(int)
    )

    # Drop rows with invalid timestamps
    d = d.dropna(subset=["ts"])
    if d.empty:
        return pd.DataFrame(
            columns=["user_id", "bucket_time", "count", "mu", "mad", "zrobust", "anom_score"]
        )

    # Get frequency string
    freq = _BUCKET_TO_FREQ.get(bucket, "1min")
    freq = _normalize_freq(freq)

    # Aggregate by bucket
    per_bucket = (
        d.set_index("ts")
        .groupby("user_id")["is_fail"]
        .resample(freq)
        .sum()
        .rename("count")
        .reset_index()
        .sort_values(["user_id", "ts"])
    )

    if per_bucket.empty:
        return pd.DataFrame(
            columns=["user_id", "bucket_time", "count", "mu", "mad", "zrobust", "anom_score"]
        )

    # EWMA baseline (adapts to drift)
    per_bucket["mu"] = per_bucket.groupby("user_id")["count"].transform(
        lambda s: s.ewm(alpha=alpha, adjust=False).mean()
    )

    # Scale rolling window proportionally to bucket size
    # Goal: Keep ~24 hours of history regardless of bucket
    bucket_minutes = _BUCKET_TO_MINUTES.get(bucket, 1)
    window_count = max(3, int(1440 / bucket_minutes))  # At least 3 buckets
    min_periods = max(3, window_count // 48)  # At least 3, or 30 minutes worth

    # Robust spread via rolling MAD around median
    mad = per_bucket.groupby("user_id")["count"].transform(
        lambda s: (np.abs(s - s.median())).rolling(window_count, min_periods=min_periods).median()
    )

    # Avoid zero MAD (use median of non-zero MADs as fallback)
    non_zero_median = mad[mad > 0].median(skipna=True)
    if pd.isna(non_zero_median) or non_zero_median == 0:
        non_zero_median = 1.0
    mad = mad.replace(0, non_zero_median)
    per_bucket["mad"] = mad.fillna(1.0)

    per_bucket["zrobust"] = (per_bucket["count"] - per_bucket["mu"]) / (
            1.4826 * (per_bucket["mad"] + 1e-6)
    )

    # Map to 0..1 for consistent coloring
    per_bucket["anom_score"] = 1 / (1 + np.exp(-np.abs(per_bucket["zrobust"])))

    per_bucket["bucket_time"] = pd.to_datetime(per_bucket["ts"], utc=True).dt.floor(freq)
    return per_bucket[["user_id", "bucket_time", "count", "mu", "mad", "zrobust", "anom_score"]]


def _hour_rarity(df: pd.DataFrame, bucket: str = "1m") -> pd.DataFrame:
    """
    Per-user hour-of-day rarity ~ empirical probability of that hour.

    Args:
        df: Input dataframe with ts, user_id columns
        bucket: Time bucket size for aggregation

    Returns: user_id, bucket_time, hour_prob, hour_rarity
    """
    d = df.copy()
    d["ts"] = pd.to_datetime(d["ts"], utc=True, errors="coerce")

    # Ensure user_id exists
    if "user_id" not in d.columns:
        d["user_id"] = "unknown"
    d["user_id"] = d["user_id"].fillna("unknown").astype(str)

    # Drop invalid timestamps
    d = d.dropna(subset=["ts"])
    if d.empty:
        return pd.DataFrame(columns=["user_id", "bucket_time", "hour_prob", "hour_rarity"])

    freq = _BUCKET_TO_FREQ.get(bucket, "1min")
    d["bucket_time"] = d["ts"].dt.floor(freq)
    d["hour"] = d["ts"].dt.hour

    # Per-user hour histogram
    counts = d.groupby(["user_id", "hour"]).size().rename("hcount").reset_index()
    totals = counts.groupby("user_id")["hcount"].sum().rename("htotal").reset_index()
    probs = counts.merge(totals, on="user_id")

    probs["hour_prob"] = probs["hcount"] / probs["htotal"].clip(lower=1)
    # Rarity:  smaller prob -> larger rarity (negative log-prob)
    probs["hour_rarity"] = (-np.log(probs["hour_prob"].clip(lower=1e-6))).astype(float)

    out = d[["user_id", "bucket_time", "hour"]].merge(
        probs[["user_id", "hour", "hour_prob", "hour_rarity"]],
        on=["user_id", "hour"],
        how="left",
    )
    out[["hour_prob", "hour_rarity"]] = out[["hour_prob", "hour_rarity"]].fillna(0.0)
    return out[["user_id", "bucket_time", "hour_prob", "hour_rarity"]]


def mark_anomalies(
        df: pd.DataFrame,
        feature: str = "count",
        z_thr: float = 3.0,
        bucket: str = "1m",
        alpha: float = 0.2,
) -> pd.DataFrame:
    """
    Detect anomalies using robust z-score (EWMA + MAD) per user at configurable granularity.

    Args:
        df: Input dataframe with ts, user_id, event_type columns
        feature:  Which column to aggregate ('count', 'is_fail', or column name)
        z_thr: Z-score threshold for flagging anomalies
        bucket:  Time bucket size ('30s', '1m', '5m', '15m', '1h', etc.)
        alpha: EWMA smoothing factor

    Returns: 
        DataFrame with added columns:  anomaly, anom_score, explain, reasons
    """
    if df.empty:
        df = df.copy()
        df["anomaly"] = False
        df["anom_score"] = 0.0
        df["explain"] = ""
        df["reasons"] = [[] for _ in range(len(df))]
        return df

    d = df.copy()
    d["ts"] = pd.to_datetime(d["ts"], utc=True, errors="coerce")

    # Ensure user_id exists
    if "user_id" not in d.columns:
        d["user_id"] = "unknown"
    d["user_id"] = d["user_id"].fillna("unknown").astype(str)

    # Ensure event_type exists
    if "event_type" not in d.columns:
        d["event_type"] = "unknown"

    # --- Feature selection ---
    if feature == "count":
        # Count all events per bucket
        d["feature_val"] = 1
    elif feature == "is_fail":
        d["feature_val"] = (
            d["event_type"]
            .astype(str)
            .str.contains("fail", case=False, na=False)
            .astype(int)
        )
    elif feature in d.columns:
        # Use the specified column (must be numeric)
        d["feature_val"] = pd.to_numeric(d[feature], errors="coerce").fillna(0)
    else:
        # Fallback:  count all events
        d["feature_val"] = 1

    # Drop rows with invalid timestamps for aggregation
    valid_ts = d["ts"].notna()
    if not valid_ts.any():
        d["anomaly"] = False
        d["anom_score"] = 0.0
        d["explain"] = ""
        d["reasons"] = [[] for _ in range(len(d))]
        return d

    # Get frequency string
    freq = _BUCKET_TO_FREQ.get(bucket, "1min")
    freq = _normalize_freq(freq)

    # Per-user per-bucket aggregation
    per_bucket = (
        d.loc[valid_ts]
        .set_index("ts")
        .groupby("user_id")["feature_val"]
        .resample(freq)
        .sum()
        .rename("count")
        .reset_index()
    )

    if per_bucket.empty:
        d["anomaly"] = False
        d["anom_score"] = 0.0
        d["explain"] = ""
        d["reasons"] = [[] for _ in range(len(d))]
        return d

    # EWMA baseline
    per_bucket["mu"] = per_bucket.groupby("user_id")["count"].transform(
        lambda s: s.ewm(alpha=alpha, adjust=False).mean()
    )

    # Scale rolling window proportionally to bucket size
    bucket_minutes = _BUCKET_TO_MINUTES.get(bucket, 1)
    window_count = max(3, int(1440 / bucket_minutes))
    min_periods = max(3, window_count // 48)

    # Robust scale via MAD (about the median)
    mad = per_bucket.groupby("user_id")["count"].transform(
        lambda s: (np.abs(s - s.median())).rolling(window_count, min_periods=min_periods).median()
    )
    non_zero_median = mad[mad > 0].median(skipna=True)
    if pd.isna(non_zero_median) or non_zero_median == 0:
        non_zero_median = 1.0
    mad = mad.replace(0, non_zero_median)

    per_bucket["zrobust"] = (per_bucket["count"] - per_bucket["mu"]) / (
            1.4826 * (mad + 1e-6)
    )

    # Sigmoid score for explainability (0..1)
    per_bucket["anom_score"] = 1 / (1 + np.exp(-np.abs(per_bucket["zrobust"])))

    # Flag anomaly
    per_bucket["anomaly"] = per_bucket["zrobust"].abs() >= z_thr

    # Explainability envelope
    def reason_row(row) -> List[Dict[str, Any]]:
        reasons: List[Dict[str, Any]] = []
        if row["anomaly"]:
            reasons.append(
                {
                    "code": "ROBUST_Z",
                    "value": float(row["zrobust"]),
                    "thr": float(z_thr),
                    "desc": f"Robust z-score {row['zrobust']:.2f} exceeds threshold {z_thr}",
                }
            )
        return reasons

    per_bucket["reasons"] = per_bucket.apply(reason_row, axis=1)
    per_bucket["explain"] = per_bucket.apply(
        lambda row: "; ".join([r["desc"] for r in row["reasons"]])
        if row["anomaly"]
        else "",
        axis=1,
    )

    # Merge anomaly envelope back to original df (by user_id + bucket)
    per_bucket["bucket_time"] = per_bucket["ts"].dt.floor(freq)
    d["bucket_time"] = d["ts"].dt.floor(freq)

    # Perform merge
    d = d.merge(
        per_bucket[["user_id", "bucket_time", "anomaly", "anom_score", "explain", "reasons"]],
        on=["user_id", "bucket_time"],
        how="left",
    )

    # Fill missing values for events without anomaly data
    d["anomaly"] = d["anomaly"].fillna(False)
    d["anom_score"] = d["anom_score"].fillna(0.0)
    d["explain"] = d["explain"].fillna("")
    d["reasons"] = d["reasons"].apply(lambda x: x if isinstance(x, list) else [])

    # Clean up temporary column
    if "bucket_time" in d.columns:
        d = d.drop(columns=["bucket_time"])
    if "feature_val" in d.columns:
        d = d.drop(columns=["feature_val"])

    return d

