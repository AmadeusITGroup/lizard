# app/viz_engine.py
"""
Engine-aware data layer for visualization and analytics endpoints.

Dispatches between the local SQLAlchemy/pandas path and the remote
Spark-on-Databricks path depending on the active cloud configuration.
All /viz/* and /analytics/* endpoints call into this module instead of
querying the database directly, ensuring a single integration point for
both execution modes.
"""
from __future__ import annotations

import time
import logging
from typing import Any, Dict, List, Optional, Literal

import pandas as pd

log = logging.getLogger("lizard.viz_engine")


# ── public API ──────────────────��────────────────────────────────────

async def get_viz_data(
    start: str,
    end: str,
    *,
    limit: int = 50_000,
    source: Optional[str] = None,
    where: Optional[List[Any]] = None,
    analytics: Literal["none", "simple", "advanced"] = "none",
    z_thr: float = 3.0,
    contamination: float = 0.05,
    speed_thr: float = 900.0,
    dist_thr: float = 2000.0,
    bucket: Optional[str] = None,
    session: Optional[Any] = None,
) -> pd.DataFrame:
    """
    Fetch events and optionally run anomaly detection.

    Cloud mode: data is fetched via Spark SQL on the configured
    Databricks cluster/warehouse. Analytics run as Spark SQL window
    functions for large datasets, with a local pandas fallback for
    smaller result sets.

    Local mode: delegates to the existing _events_df + pandas analytics
    path with zero behaviour change.
    """
    if _is_cloud_mode():
        return await _cloud_fetch(
            start=start, end=end, limit=limit, source=source,
            where=where, analytics=analytics, z_thr=z_thr,
            contamination=contamination, speed_thr=speed_thr,
            dist_thr=dist_thr, bucket=bucket,
        )

    # Local mode
    from app.main import _events_df_local, _apply_where
    df = await _events_df_local(session, start, end, limit=limit, source=source)
    if where:
        df = _apply_where(df, where)
    if not df.empty:
        df = _local_analytics(df, analytics, z_thr, contamination, speed_thr, dist_thr, bucket)
    return df


async def get_viz_data_raw(
    start: str,
    end: str,
    *,
    limit: int = 50_000,
    source: Optional[str] = None,
    where: Optional[List[Any]] = None,
    session: Optional[Any] = None,
) -> pd.DataFrame:
    """Fetch events without analytics."""
    return await get_viz_data(
        start=start, end=end, limit=limit, source=source,
        where=where, analytics="none", session=session,
    )


# ── cloud-mode detection ─────────────────────────────────────────────

def _is_cloud_mode() -> bool:
    """Return True when cloud mode is active AND a Spark compute resource is configured."""
    try:
        from cloud.config import get_config
        cfg = get_config()
        if not cfg.is_cloud_mode:
            return False
        for conn in cfg.databricks_connections:
            if getattr(conn, "cluster_id", None) or getattr(conn, "warehouse_id", None):
                return True
        return False
    except Exception:
        return False


# ── local analytics helper ───────────────────────────────────────────

def _local_analytics(
    df: pd.DataFrame,
    analytics: str,
    z_thr: float,
    contamination: float,
    speed_thr: float,
    dist_thr: float,
    bucket: Optional[str],
) -> pd.DataFrame:
    """Run analytics locally via the existing pandas functions."""
    if analytics == "simple":
        from analytics.simple_anomaly import mark_anomalies
        return mark_anomalies(df, z_thr=z_thr, bucket=bucket or "1m")
    if analytics == "advanced":
        from analytics.advanced_anomaly import compute_advanced_anomaly
        return compute_advanced_anomaly(
            df, contamination=contamination,
            speed_kmh_thr=speed_thr, dist_km_thr=dist_thr,
        )
    return df


# ── cloud (Spark) data fetch ─────────────────────────────────────────

async def _cloud_fetch(
    start: str, end: str, limit: int,
    source: Optional[str], where: Optional[List[Any]],
    analytics: str, z_thr: float, contamination: float,
    speed_thr: float, dist_thr: float, bucket: Optional[str],
) -> pd.DataFrame:
    """Build a Spark SQL pipeline, execute on Databricks, and return a pandas DataFrame."""
    from cloud.execution.engine_factory import get_engine

    engine = get_engine()
    t0 = time.perf_counter()

    # Assemble the pipeline
    pipeline: List[Dict[str, Any]] = []
    table_name = source or "events"
    pipeline.append({"type": "source", "config": {"table": table_name}})
    pipeline.append({
        "type": "filter",
        "config": {"conditions": [
            {"field": "ts", "op": "gte", "value": start},
            {"field": "ts", "op": "lte", "value": end},
        ]},
    })
    if where:
        spark_conds = _translate_where(where)
        if spark_conds:
            pipeline.append({"type": "filter", "config": {"conditions": spark_conds}})
    pipeline.append({"type": "limit", "config": {"n": limit}})

    result = await engine.execute_pipeline(pipeline, limit=limit, offset=0)
    df = result.data
    log.info("cloud_fetch", rows=len(df), ms=round((time.perf_counter() - t0) * 1000, 1))

    if df.empty:
        return df

    # Analytics: for large datasets try Spark-native z-score; otherwise pandas
    if analytics != "none":
        if analytics == "simple" and len(df) > 100_000:
            try:
                df = await _spark_simple_anomaly(
                    engine, table_name, start, end, where, limit, z_thr, bucket or "1m",
                )
            except Exception as exc:
                log.warning("spark_analytics_fallback", error=str(exc))
                df = _local_analytics(df, analytics, z_thr, contamination, speed_thr, dist_thr, bucket)
        else:
            df = _local_analytics(df, analytics, z_thr, contamination, speed_thr, dist_thr, bucket)

    log.info("cloud_fetch_done", rows=len(df), analytics=analytics,
             ms=round((time.perf_counter() - t0) * 1000, 1))
    return df


# ── Spark-native simple anomaly detection ─────────────────────────────

async def _spark_simple_anomaly(
    engine: Any, table_name: str, start: str, end: str,
    where: Optional[List[Any]], limit: int, z_thr: float, bucket: str,
) -> pd.DataFrame:
    """
    Compute z-score anomaly detection via Spark SQL window functions.

    Equivalent to analytics.simple_anomaly.mark_anomalies but executed
    entirely on the remote cluster for datasets that would be expensive
    to transfer and process locally.
    """
    bucket_interval = _bucket_to_spark_interval(bucket)
    where_sql = _build_where_sql(where)
    extra = f"AND {where_sql}" if where_sql else ""

    sql = f"""
    WITH base AS (
        SELECT *, date_trunc('{bucket_interval}', ts) AS _bucket
        FROM {table_name}
        WHERE ts >= '{start}' AND ts <= '{end}' {extra}
        LIMIT {limit}
    ),
    bucket_counts AS (
        SELECT _bucket, user_id, COUNT(*) AS cnt
        FROM base GROUP BY _bucket, user_id
    ),
    stats AS (
        SELECT _bucket, user_id, cnt,
               AVG(cnt) OVER (PARTITION BY user_id ORDER BY _bucket
                              ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS rolling_avg,
               STDDEV(cnt) OVER (PARTITION BY user_id ORDER BY _bucket
                                 ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS rolling_std
        FROM bucket_counts
    ),
    scored AS (
        SELECT *,
               CASE WHEN rolling_std > 0 THEN (cnt - rolling_avg) / rolling_std ELSE 0.0 END AS zscore,
               CASE WHEN rolling_std > 0 AND (cnt - rolling_avg) / rolling_std > {z_thr}
                    THEN TRUE ELSE FALSE END AS anomaly
        FROM stats
    )
    SELECT b.*,
           sc.zscore,
           sc.anomaly,
           1.0 / (1.0 + EXP(-COALESCE(sc.zscore, 0))) AS anom_score
    FROM base b
    LEFT JOIN scored sc ON b.user_id = sc.user_id
                        AND date_trunc('{bucket_interval}', b.ts) = sc._bucket
    """

    client = engine._get_client()
    if engine._warehouse_id:
        df, _ = engine._execute_via_warehouse(client, sql)
    elif engine._cluster_id:
        df, _ = engine._execute_via_cluster(client, sql)
    else:
        raise RuntimeError("No Spark compute resource configured")
    return df


# ── translation helpers ──────────────────────────────────────────────

def _translate_where(where: List[Any]) -> List[Dict[str, Any]]:
    """Convert user-supplied filter objects to pipeline condition dicts."""
    from app.main import _normalize_cond
    out = []
    for raw in where:
        c = _normalize_cond(raw)
        if c:
            out.append(c)
    return out


def _build_where_sql(where: Optional[List[Any]]) -> str:
    """Render user conditions as a SQL fragment."""
    if not where:
        return ""
    from app.main import _normalize_cond

    def _q(v: Any) -> str:
        if isinstance(v, str):
            return f"'{v.replace(chr(39), chr(39) + chr(39))}'"
        return str(v)

    clauses: List[str] = []
    for raw in where:
        c = _normalize_cond(raw)
        if not c:
            continue
        f, op, v = c["field"], c["op"], c.get("value")
        simple = {
            "eq": f"{f} = {_q(v)}", "ne": f"{f} != {_q(v)}",
            "gt": f"{f} > {_q(v)}", "gte": f"{f} >= {_q(v)}",
            "lt": f"{f} < {_q(v)}", "lte": f"{f} <= {_q(v)}",
            "contains": f"{f} LIKE '%{v}%'",
            "startswith": f"{f} LIKE '{v}%'",
            "endswith": f"{f} LIKE '%{v}'",
            "isnull": f"{f} IS NULL", "notnull": f"{f} IS NOT NULL",
        }
        if op == "in" and isinstance(v, list):
            clauses.append(f"{f} IN ({', '.join(_q(x) for x in v)})")
        elif op == "nin" and isinstance(v, list):
            clauses.append(f"{f} NOT IN ({', '.join(_q(x) for x in v)})")
        elif op in simple:
            clauses.append(simple[op])
    return " AND ".join(clauses)


def _bucket_to_spark_interval(bucket: str) -> str:
    """Map a Lizard bucket label to a Spark SQL date_trunc unit."""
    return {
        "30s": "minute", "1m": "minute", "2m": "minute", "5m": "minute",
        "10m": "minute", "15m": "minute", "30m": "minute",
        "1h": "hour", "3h": "hour", "6h": "hour", "12h": "hour", "1d": "day",
    }.get(bucket, "minute")