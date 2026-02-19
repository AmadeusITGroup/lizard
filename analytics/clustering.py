# path: analytics/clustering.py
from __future__ import annotations
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler


def cluster_geo_temporal(
    df: pd.DataFrame, eps: float = 0.7, min_samples:  int = 15
) -> pd.DataFrame:
    """
    Cluster by lat/lon and time using DBSCAN.
    Features:  [lat, lon, minutes_since_start]
    eps is in standardized feature space.
    """
    if df.empty:
        df["cluster"] = -1
        return df

    # Create a copy to avoid modifying original
    result = df.copy()
    result["cluster"] = -1  # Default:  no cluster

    # Filter rows with valid geo coordinates
    geo_mask = result["geo_lat"].notna() & result["geo_lon"].notna()
    if not geo_mask.any():
        return result

    # Work with subset that has geo data
    d = result.loc[geo_mask].copy()
    d["ts"] = pd.to_datetime(d["ts"], utc=True, errors="coerce")

    # Drop rows where ts parsing failed
    valid_ts_mask = d["ts"].notna()
    if not valid_ts_mask.any():
        return result

    d = d.loc[valid_ts_mask].copy()

    t0 = d["ts"].min()
    d["mins"] = (d["ts"] - t0).dt.total_seconds() / 60.0

    X = d[["geo_lat", "geo_lon", "mins"]].to_numpy(dtype=float)

    # Handle edge case:  not enough samples for clustering
    if len(X) < min_samples:
        return result

    Xs = StandardScaler().fit_transform(X)
    labels = DBSCAN(eps=eps, min_samples=min_samples).fit_predict(Xs)

    # Assign cluster labels back using the original index
    result.loc[d.index, "cluster"] = labels

    return result