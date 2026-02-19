# tests/test_execution.py
"""
Tests for the Phase 3 execution engine abstraction.

Covers:
  - ExecutionResult dataclass and serialization
  - ExecutionEngine ABC (cannot be instantiated)
  - LocalPandasEngine (wraps existing PipelineExecutor, no behavior change)
  - SparkDatabricksEngine (mocked — no real Databricks cluster needed)
  - engine_factory: get_engine() and reset_engines()
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
import pytest
import pandas as pd
import numpy as np
from datetime import datetime


# ── ExecutionResult ──────────────────────────────────────────────────


def test_execution_result_creation():
    from cloud.execution.base import ExecutionResult

    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    result = ExecutionResult(
        data=df,
        total_rows=3,
        columns=["a", "b"],
        engine="local",
        execution_time_ms=12.5,
    )
    assert result.engine == "local"
    assert result.total_rows == 3
    assert result.cluster_id is None
    assert result.warnings == []
    assert result.metadata == {}


def test_execution_result_to_api_dict():
    from cloud.execution.base import ExecutionResult

    df = pd.DataFrame({"x": [10, 20], "y": ["hello", "world"]})
    result = ExecutionResult(
        data=df,
        total_rows=2,
        columns=["x", "y"],
        engine="local",
        execution_time_ms=5.0,
    )
    api = result.to_api_dict(limit=100, offset=0)
    assert api["row_count"] == 2
    assert api["total_rows"] == 2
    assert api["engine"] == "local"
    assert api["limit"] == 100
    assert api["offset"] == 0
    assert len(api["data"]) == 2
    assert api["columns"] == ["x", "y"]
    assert api["data"][0]["x"] == 10
    assert api["data"][1]["y"] == "world"


def test_execution_result_to_api_dict_handles_nan():
    from cloud.execution.base import ExecutionResult

    df = pd.DataFrame({"val": [1.0, np.nan, 3.0]})
    result = ExecutionResult(
        data=df, total_rows=3, columns=["val"], engine="local"
    )
    api = result.to_api_dict()
    assert api["data"][1]["val"] is None


def test_execution_result_to_api_dict_handles_datetime():
    from cloud.execution.base import ExecutionResult

    ts = datetime(2025, 6, 15, 12, 30, 0)
    df = pd.DataFrame({"ts": [ts], "val": [42]})
    result = ExecutionResult(
        data=df, total_rows=1, columns=["ts", "val"], engine="local"
    )
    api = result.to_api_dict()
    assert api["data"][0]["ts"] == "2025-06-15T12:30:00"


def test_execution_result_to_api_dict_handles_numpy_scalar():
    from cloud.execution.base import ExecutionResult

    df = pd.DataFrame({"count": [np.int64(42)]})
    result = ExecutionResult(
        data=df, total_rows=1, columns=["count"], engine="local"
    )
    api = result.to_api_dict()
    assert api["data"][0]["count"] == 42
    assert isinstance(api["data"][0]["count"], int)


# ── ExecutionEngine ABC ──────────────────────────────────────────────


def test_execution_engine_is_abstract():
    from cloud.execution.base import ExecutionEngine

    with pytest.raises(TypeError):
        ExecutionEngine()


# ── LocalPandasEngine ────────────────────────────────────────────────


def test_local_engine_name():
    from cloud.execution.local_engine import LocalPandasEngine

    engine = LocalPandasEngine()
    assert engine.engine_name() == "local"


@pytest.mark.asyncio
async def test_local_engine_health_check():
    from cloud.execution.local_engine import LocalPandasEngine

    engine = LocalPandasEngine()
    health = await engine.health_check()
    assert health["engine"] == "local"
    assert health["status"] == "ok"


@pytest.mark.asyncio
async def test_local_engine_execute_pipeline():
    """
    LocalPandasEngine.execute_pipeline delegates to PipelineExecutor.
    We mock PipelineExecutor.execute to avoid needing a real database.
    """
    from cloud.execution.local_engine import LocalPandasEngine
    from cloud.execution.base import ExecutionResult

    engine = LocalPandasEngine()

    mock_df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})

    # Mock the executor that gets lazy-loaded
    mock_executor = MagicMock()
    mock_executor.execute = AsyncMock(return_value=(mock_df, 3))
    engine._executor = mock_executor

    pipeline = [{"type": "source", "config": {"table": "test_data"}}]
    result = await engine.execute_pipeline(pipeline, limit=100, offset=0)

    assert isinstance(result, ExecutionResult)
    assert result.engine == "local"
    assert result.total_rows == 3
    assert len(result.data) == 3
    assert result.columns == ["id", "value"]
    assert result.execution_time_ms >= 0


@pytest.mark.asyncio
async def test_local_engine_preview_pipeline():
    """preview_pipeline delegates to execute_pipeline with limit."""
    from cloud.execution.local_engine import LocalPandasEngine

    engine = LocalPandasEngine()

    mock_df = pd.DataFrame({"x": [1, 2]})
    mock_executor = MagicMock()
    mock_executor.execute = AsyncMock(return_value=(mock_df, 2))
    engine._executor = mock_executor

    pipeline = [{"type": "source", "config": {"table": "preview_test"}}]
    result = await engine.preview_pipeline(pipeline, limit=10)

    assert result.engine == "local"
    assert result.total_rows == 2


@pytest.mark.asyncio
async def test_local_engine_execute_pipeline_empty():
    """Handles empty result gracefully."""
    from cloud.execution.local_engine import LocalPandasEngine

    engine = LocalPandasEngine()

    empty_df = pd.DataFrame()
    mock_executor = MagicMock()
    mock_executor.execute = AsyncMock(return_value=(empty_df, 0))
    engine._executor = mock_executor

    pipeline = [{"type": "source", "config": {"table": "empty"}}]
    result = await engine.execute_pipeline(pipeline)

    assert result.total_rows == 0
    assert len(result.data) == 0


# ── SparkDatabricksEngine ───────────────────────────────────────────


def test_spark_engine_name():
    from cloud.execution.spark_engine import SparkDatabricksEngine

    engine = SparkDatabricksEngine(
        connection_name="test_conn",
        cluster_id="abc-123",
    )
    assert engine.engine_name() == "spark"


def test_spark_engine_pipeline_to_sql_source():
    """Test pipeline → SQL translation: simple source."""
    from cloud.execution.spark_engine import SparkDatabricksEngine

    engine = SparkDatabricksEngine(connection_name="test", cluster_id="c1")
    pipeline = [{"type": "source", "config": {"table": "transactions"}}]
    sql = engine._pipeline_to_sql(pipeline, limit=100, offset=0)

    assert "transactions" in sql
    assert "LIMIT 100" in sql
    assert "OFFSET 0" in sql


def test_spark_engine_pipeline_to_sql_filter():
    """Test pipeline → SQL translation: source + filter."""
    from cloud.execution.spark_engine import SparkDatabricksEngine

    engine = SparkDatabricksEngine(connection_name="test", cluster_id="c1")
    pipeline = [
        {"type": "source", "config": {"table": "events"}},
        {
            "type": "filter",
            "config": {
                "conditions": [
                    {"field": "country", "op": "eq", "value": "FR"},
                    {"field": "amount", "op": "gt", "value": 100},
                ]
            },
        },
    ]
    sql = engine._pipeline_to_sql(pipeline, limit=50, offset=0)

    assert "country = 'FR'" in sql
    assert "amount > 100" in sql
    assert "LIMIT 50" in sql


def test_spark_engine_pipeline_to_sql_aggregate():
    """Test pipeline → SQL translation: source + aggregate."""
    from cloud.execution.spark_engine import SparkDatabricksEngine

    engine = SparkDatabricksEngine(connection_name="test", cluster_id="c1")
    pipeline = [
        {"type": "source", "config": {"table": "events"}},
        {
            "type": "aggregate",
            "config": {
                "group_by": ["country"],
                "aggregations": [
                    {"column": "total", "func": "count", "field": "*"},
                    {"column": "avg_amount", "func": "avg", "field": "amount"},
                ],
            },
        },
    ]
    sql = engine._pipeline_to_sql(pipeline, limit=1000, offset=0)

    assert "GROUP BY country" in sql
    assert "COUNT(*) AS total" in sql
    assert "AVG(amount) AS avg_amount" in sql


def test_spark_engine_pipeline_to_sql_sort():
    """Test pipeline → SQL translation: source + sort."""
    from cloud.execution.spark_engine import SparkDatabricksEngine

    engine = SparkDatabricksEngine(connection_name="test", cluster_id="c1")
    pipeline = [
        {"type": "source", "config": {"table": "events"}},
        {
            "type": "sort",
            "config": {
                "by": [
                    {"field": "ts", "direction": "desc"},
                    {"field": "amount", "direction": "asc"},
                ]
            },
        },
    ]
    sql = engine._pipeline_to_sql(pipeline, limit=100, offset=0)

    assert "ORDER BY ts DESC, amount ASC" in sql


def test_spark_engine_pipeline_to_sql_select():
    """Test pipeline → SQL translation: source + select with alias."""
    from cloud.execution.spark_engine import SparkDatabricksEngine

    engine = SparkDatabricksEngine(connection_name="test", cluster_id="c1")
    pipeline = [
        {"type": "source", "config": {"table": "events"}},
        {
            "type": "select",
            "config": {
                "columns": [
                    "country",
                    {"source": "amount", "alias": "total_amount"},
                ]
            },
        },
    ]
    sql = engine._pipeline_to_sql(pipeline, limit=100, offset=0)

    assert "country" in sql
    assert "amount AS total_amount" in sql


def test_spark_engine_pipeline_to_sql_join():
    """Test pipeline → SQL translation: source + join."""
    from cloud.execution.spark_engine import SparkDatabricksEngine

    engine = SparkDatabricksEngine(connection_name="test", cluster_id="c1")
    pipeline = [
        {"type": "source", "config": {"table": "orders"}},
        {
            "type": "join",
            "config": {
                "table": "customers",
                "type": "left",
                "on": [{"left": "user_id", "right": "id"}],
            },
        },
    ]
    sql = engine._pipeline_to_sql(pipeline, limit=100, offset=0)

    assert "LEFT JOIN customers" in sql
    assert "user_id" in sql


def test_spark_engine_pipeline_to_sql_distinct():
    """Test pipeline → SQL translation: source + distinct."""
    from cloud.execution.spark_engine import SparkDatabricksEngine

    engine = SparkDatabricksEngine(connection_name="test", cluster_id="c1")
    pipeline = [
        {"type": "source", "config": {"table": "events"}},
        {"type": "distinct", "config": {}},
    ]
    sql = engine._pipeline_to_sql(pipeline, limit=100, offset=0)

    assert "DISTINCT" in sql


def test_spark_engine_pipeline_to_sql_empty():
    """Empty pipeline returns a safe fallback query."""
    from cloud.execution.spark_engine import SparkDatabricksEngine

    engine = SparkDatabricksEngine(connection_name="test", cluster_id="c1")
    sql = engine._pipeline_to_sql([], limit=100, offset=0)

    assert "_empty" in sql


def test_spark_engine_condition_to_sql():
    """Test individual condition → SQL fragment conversion."""
    from cloud.execution.spark_engine import SparkDatabricksEngine

    assert SparkDatabricksEngine._condition_to_sql(
        {"field": "name", "op": "eq", "value": "Alice"}
    ) == "name = 'Alice'"

    assert SparkDatabricksEngine._condition_to_sql(
        {"field": "age", "op": "gte", "value": 18}
    ) == "age >= 18"

    assert SparkDatabricksEngine._condition_to_sql(
        {"field": "status", "op": "isnull"}
    ) == "status IS NULL"

    assert SparkDatabricksEngine._condition_to_sql(
        {"field": "name", "op": "contains", "value": "test"}
    ) == "name LIKE '%test%'"

    assert SparkDatabricksEngine._condition_to_sql(
        {"field": "country", "op": "in", "value": ["FR", "DE", "IT"]}
    ) == "country IN ('FR', 'DE', 'IT')"

    assert SparkDatabricksEngine._condition_to_sql(
        {"field": "amount", "op": "between", "value": [100, 500]}
    ) == "amount BETWEEN 100 AND 500"

    assert SparkDatabricksEngine._condition_to_sql(
        {"field": "", "op": "eq", "value": "x"}
    ) is None


@pytest.mark.asyncio
async def test_spark_engine_health_check_no_client():
    """health_check returns error when client cannot be created."""
    from cloud.execution.spark_engine import SparkDatabricksEngine

    engine = SparkDatabricksEngine(connection_name="nonexistent", cluster_id="c1")

    # _get_client will fail because the connection doesn't exist in config
    with patch.object(engine, "_get_client", side_effect=Exception("Connection not found")):
        health = await engine.health_check()

    assert health["status"] == "error"
    assert "Connection not found" in health["error"]


@pytest.mark.asyncio
async def test_spark_engine_execute_no_compute():
    """execute_pipeline fails gracefully when no cluster or warehouse is set."""
    from cloud.execution.spark_engine import SparkDatabricksEngine
    from cloud.diagnostics import ConfigurationError

    engine = SparkDatabricksEngine(
        connection_name="test",
        cluster_id=None,
        warehouse_id=None,
    )

    # Mock _get_client to return a mock client (so we get past auth)
    engine._client = MagicMock()

    pipeline = [{"type": "source", "config": {"table": "test"}}]
    with pytest.raises(ConfigurationError, match="No cluster_id or warehouse_id"):
        await engine.execute_pipeline(pipeline)


# ── engine_factory ───────────────────────────────────────────────────


def test_engine_factory_local_mode():
    """In local mode, get_engine() returns LocalPandasEngine."""
    from cloud.execution.engine_factory import get_engine, reset_engines
    from cloud.execution.local_engine import LocalPandasEngine

    reset_engines()

    with patch("cloud.config.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            is_cloud_mode=False,
            databricks_connections=[],
        )
        engine = get_engine()

    assert isinstance(engine, LocalPandasEngine)
    assert engine.engine_name() == "local"

    reset_engines()


def test_engine_factory_cloud_mode_no_compute():
    """In cloud mode without compute config, falls back to LocalPandasEngine."""
    from cloud.execution.engine_factory import get_engine, reset_engines
    from cloud.execution.local_engine import LocalPandasEngine

    reset_engines()

    mock_conn = MagicMock()
    mock_conn.name = "my_workspace"
    mock_conn.cluster_id = None
    mock_conn.warehouse_id = None

    with patch("cloud.config.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            is_cloud_mode=True,
            databricks_connections=[mock_conn],
        )
        engine = get_engine()

    assert isinstance(engine, LocalPandasEngine)

    reset_engines()


def test_engine_factory_cloud_mode_with_cluster():
    """In cloud mode with cluster_id, returns SparkDatabricksEngine."""
    from cloud.execution.engine_factory import get_engine, reset_engines
    from cloud.execution.spark_engine import SparkDatabricksEngine

    reset_engines()

    mock_conn = MagicMock()
    mock_conn.name = "my_workspace"
    mock_conn.cluster_id = "cluster-abc-123"
    mock_conn.warehouse_id = None

    with patch("cloud.config.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            is_cloud_mode=True,
            databricks_connections=[mock_conn],
        )
        engine = get_engine()

    assert isinstance(engine, SparkDatabricksEngine)
    assert engine.engine_name() == "spark"
    assert engine._connection_name == "my_workspace"
    assert engine._cluster_id == "cluster-abc-123"

    reset_engines()


def test_engine_factory_cloud_mode_with_warehouse():
    """In cloud mode with warehouse_id, returns SparkDatabricksEngine."""
    from cloud.execution.engine_factory import get_engine, reset_engines
    from cloud.execution.spark_engine import SparkDatabricksEngine

    reset_engines()

    mock_conn = MagicMock()
    mock_conn.name = "sql_workspace"
    mock_conn.cluster_id = None
    mock_conn.warehouse_id = "wh-xyz-789"

    with patch("cloud.config.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            is_cloud_mode=True,
            databricks_connections=[mock_conn],
        )
        engine = get_engine()

    assert isinstance(engine, SparkDatabricksEngine)
    assert engine._warehouse_id == "wh-xyz-789"

    reset_engines()


def test_engine_factory_reset():
    """reset_engines clears cached engines."""
    from cloud.execution.engine_factory import get_engine, reset_engines
    import cloud.execution.engine_factory as factory

    reset_engines()
    assert factory._local_engine is None
    assert factory._spark_engine is None

    with patch("cloud.config.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            is_cloud_mode=False,
            databricks_connections=[],
        )
        engine = get_engine()

    assert factory._local_engine is not None

    reset_engines()
    assert factory._local_engine is None
    assert factory._spark_engine is None


def test_engine_factory_reuses_cached_spark():
    """get_engine() reuses cached SparkEngine if same connection name."""
    from cloud.execution.engine_factory import get_engine, reset_engines
    from cloud.execution.spark_engine import SparkDatabricksEngine

    reset_engines()

    mock_conn = MagicMock()
    mock_conn.name = "ws1"
    mock_conn.cluster_id = "c1"
    mock_conn.warehouse_id = None

    with patch("cloud.config.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            is_cloud_mode=True,
            databricks_connections=[mock_conn],
        )
        engine1 = get_engine()
        engine2 = get_engine()

    assert engine1 is engine2  # Same cached instance

    reset_engines()