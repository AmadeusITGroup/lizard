# cloud/analytics_engine.py
"""
Cloud-aware analytics engine.

Provides engine-aware wrappers for:
  - Simple anomaly detection (EWMA + MAD z-score)
  - Advanced anomaly detection (Isolation Forest)
  - Geo-temporal clustering (DBSCAN)

In local mode: delegates to existing pandas-based analytics functions.
In cloud/Spark mode: translates to Spark SQL or runs via Databricks Jobs,
  then collects results back as pandas for the API response.

Phase 4 starts with local-mode wrappers that go through the execution
engine to fetch data, then apply existing analytics. Spark-native
analytics will be added in a future iteration.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

log = structlog.get_logger(__name__)


async def run_anomaly_detection(
    pipeline: List[Dict[str, Any]],
    *,
    method: str = "simple",
    limit: int = 50000,
    # Simple anomaly params
    feature: str = "count",
    z_thr: float = 3.0,
    bucket: str = "1m",
    alpha: float = 0.2,
    # Advanced anomaly params
    contamination: float = 0.05,
    speed_kmh_thr: float = 900.0,
    dist_km_thr: float = 2000.0,
    event_velocity_thr: float = 10.0,
    rare_hour_prob: float = 0.05,
) -> Dict[str, Any]:
    """
    Run anomaly detection on pipeline results.

    Uses the execution engine to fetch data, then applies
    the appropriate anomaly detection algorithm.

    Args:
        pipeline: Pipeline steps to execute first.
        method: 'simple' (EWMA+MAD) or 'advanced' (IsolationForest).
        limit: Max rows to fetch for analysis.
        **kwargs: Parameters for the anomaly detection algorithm.

    Returns:
        Dict with anomaly results, stats, and metadata.
    """
    from cloud.execution.engine_factory import get_engine

    t0 = time.perf_counter()

    # Step 1: Execute pipeline to get data
    eng = get_engine()
    result = await eng.execute_pipeline(pipeline, limit=limit, offset=0)
    df = result.data

    if df.empty:
        return {
            "status": "ok",
            "method": method,
            "engine": result.engine,
            "total_rows": 0,
            "anomaly_count": 0,
            "data": [],
            "stats": {},
            "execution_time_ms": 0,
        }

    # Step 2: Apply anomaly detection (pandas-based for now)
    if method == "simple":
        from analytics.simple_anomaly import mark_anomalies

        df_result = mark_anomalies(
            df, feature=feature, z_thr=z_thr, bucket=bucket, alpha=alpha
        )
    elif method == "advanced":
        from analytics.advanced_anomaly import compute_advanced_anomaly

        df_result = compute_advanced_anomaly(
            df,
            contamination=contamination,
            speed_kmh_thr=speed_kmh_thr,
            dist_km_thr=dist_km_thr,
            event_velocity_thr=event_velocity_thr,
            rare_hour_prob=rare_hour_prob,
        )
    else:
        raise ValueError(f"Unknown anomaly method: {method}. Use 'simple' or 'advanced'.")

    elapsed_ms = (time.perf_counter() - t0) * 1000

    # Step 3: Build response
    anomaly_mask = df_result.get("anomaly", pd.Series(dtype=bool))
    anomaly_count = int(anomaly_mask.sum()) if not anomaly_mask.empty else 0

    # Compute stats
    stats: Dict[str, Any] = {
        "total_rows": len(df_result),
        "anomaly_count": anomaly_count,
        "anomaly_rate": round(anomaly_count / len(df_result), 4) if len(df_result) > 0 else 0,
    }

    if "anom_score" in df_result.columns:
        scores = df_result["anom_score"].dropna()
        if len(scores) > 0:
            stats["score_mean"] = round(float(scores.mean()), 4)
            stats["score_max"] = round(float(scores.max()), 4)
            stats["score_min"] = round(float(scores.min()), 4)
            stats["score_p95"] = round(float(scores.quantile(0.95)), 4)

    # Serialize anomalous rows for the response (limit to 500 for API)
    import numpy as np
    from datetime import datetime as dt

    anomalous_df = df_result[anomaly_mask].head(500) if anomaly_count > 0 else df_result.head(0)
    records = anomalous_df.replace({np.nan: None}).to_dict(orient="records")

    for record in records:
        for key, value in record.items():
            if isinstance(value, (dt, pd.Timestamp)):
                record[key] = value.isoformat()
            elif hasattr(value, "item"):
                record[key] = value.item()

    log.info(
        "anomaly_detection_complete",
        method=method,
        engine=result.engine,
        rows=len(df_result),
        anomalies=anomaly_count,
        ms=round(elapsed_ms, 1),
    )

    return {
        "status": "ok",
        "method": method,
        "engine": result.engine,
        "total_rows": len(df_result),
        "anomaly_count": anomaly_count,
        "anomalous_rows": records,
        "stats": stats,
        "columns": list(df_result.columns),
        "execution_time_ms": round(elapsed_ms, 1),
    }


async def run_clustering(
    pipeline: List[Dict[str, Any]],
    *,
    limit: int = 50000,
    eps: float = 0.7,
    min_samples: int = 15,
) -> Dict[str, Any]:
    """
    Run geo-temporal clustering on pipeline results.

    Uses the execution engine to fetch data, then applies DBSCAN clustering.

    Args:
        pipeline: Pipeline steps to execute first.
        limit: Max rows to fetch for analysis.
        eps: DBSCAN epsilon (in standardized feature space).
        min_samples: Minimum samples per cluster.

    Returns:
        Dict with clustering results.
    """
    from cloud.execution.engine_factory import get_engine

    t0 = time.perf_counter()

    eng = get_engine()
    result = await eng.execute_pipeline(pipeline, limit=limit, offset=0)
    df = result.data

    if df.empty:
        return {
            "status": "ok",
            "engine": result.engine,
            "total_rows": 0,
            "num_clusters": 0,
            "noise_count": 0,
            "data": [],
            "cluster_stats": [],
            "execution_time_ms": 0,
        }

    # Apply clustering
    from analytics.clustering import cluster_geo_temporal

    df_result = cluster_geo_temporal(df, eps=eps, min_samples=min_samples)

    elapsed_ms = (time.perf_counter() - t0) * 1000

    # Build cluster stats
    cluster_col = df_result.get("cluster", pd.Series(dtype=int))
    unique_clusters = sorted(cluster_col.unique())
    num_clusters = len([c for c in unique_clusters if c >= 0])
    noise_count = int((cluster_col == -1).sum())

    cluster_stats = []
    for cid in unique_clusters:
        if cid < 0:
            continue
        mask = cluster_col == cid
        subset = df_result[mask]
        stat: Dict[str, Any] = {
            "cluster_id": int(cid),
            "size": int(mask.sum()),
        }
        if "geo_lat" in subset.columns and "geo_lon" in subset.columns:
            stat["centroid_lat"] = round(float(subset["geo_lat"].mean()), 4)
            stat["centroid_lon"] = round(float(subset["geo_lon"].mean()), 4)
        if "ts" in subset.columns:
            ts_col = pd.to_datetime(subset["ts"], utc=True, errors="coerce").dropna()
            if len(ts_col) > 0:
                stat["time_start"] = ts_col.min().isoformat()
                stat["time_end"] = ts_col.max().isoformat()
        cluster_stats.append(stat)

    # Serialize data (limit to 5000 for API)
    import numpy as np
    from datetime import datetime as dt

    export_df = df_result.head(5000)
    records = export_df.replace({np.nan: None}).to_dict(orient="records")
    for record in records:
        for key, value in record.items():
            if isinstance(value, (dt, pd.Timestamp)):
                record[key] = value.isoformat()
            elif hasattr(value, "item"):
                record[key] = value.item()

    log.info(
        "clustering_complete",
        engine=result.engine,
        rows=len(df_result),
        clusters=num_clusters,
        noise=noise_count,
        ms=round(elapsed_ms, 1),
    )

    return {
        "status": "ok",
        "engine": result.engine,
        "total_rows": len(df_result),
        "num_clusters": num_clusters,
        "noise_count": noise_count,
        "data": records,
        "cluster_stats": cluster_stats,
        "execution_time_ms": round(elapsed_ms, 1),
    }