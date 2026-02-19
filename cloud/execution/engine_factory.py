# cloud/execution/engine_factory.py
"""
Factory for creating the appropriate ExecutionEngine based on cloud config.
"""
from __future__ import annotations

from typing import Optional

import structlog

from cloud.execution.base import ExecutionEngine
from cloud.execution.local_engine import LocalPandasEngine

log = structlog.get_logger(__name__)

# Singleton engines (avoid recreating on every request)
_local_engine: Optional[LocalPandasEngine] = None
_spark_engine: Optional[ExecutionEngine] = None


def get_engine() -> ExecutionEngine:
    """
    Return the appropriate ExecutionEngine based on current cloud config.

    - mode == 'local'  → LocalPandasEngine (always available)
    - mode == 'cloud'  → SparkDatabricksEngine if a Databricks connection
                          with a cluster/warehouse is configured,
                          otherwise falls back to LocalPandasEngine with a warning
    """
    global _local_engine, _spark_engine

    from cloud.config import get_config

    cfg = get_config()

    # ── local mode ───────────────────────────────────────────
    if not cfg.is_cloud_mode:
        if _local_engine is None:
            _local_engine = LocalPandasEngine()
        return _local_engine

    # ── cloud mode ───────────────────────────────────────────
    # Find the first Databricks connection that has a cluster or warehouse configured
    for conn in cfg.databricks_connections:
        cluster_id = getattr(conn, "cluster_id", None)
        warehouse_id = getattr(conn, "warehouse_id", None)
        if cluster_id or warehouse_id:
            log.info(
                "using_spark_engine",
                connection=conn.name,
                cluster_id=cluster_id,
                warehouse_id=warehouse_id,
            )
            # Re-create if connection changed
            if _spark_engine is not None:
                existing_name = getattr(_spark_engine, "_connection_name", None)
                if existing_name == conn.name:
                    return _spark_engine

            from cloud.execution.spark_engine import SparkDatabricksEngine

            _spark_engine = SparkDatabricksEngine(
                connection_name=conn.name,
                cluster_id=cluster_id,
                warehouse_id=warehouse_id,
            )
            return _spark_engine

    # Cloud mode but no Databricks compute configured — fall back to local
    log.warning(
        "cloud_mode_no_compute",
        msg="Cloud mode active but no Databricks cluster/warehouse configured. "
            "Falling back to local pandas engine.",
    )
    if _local_engine is None:
        _local_engine = LocalPandasEngine()
    return _local_engine


def reset_engines() -> None:
    """Reset cached engines (useful after config changes)."""
    global _local_engine, _spark_engine
    _local_engine = None
    _spark_engine = None
    log.info("execution_engines_reset")