# tests/test_analytics_engine.py
"""
Tests for cloud/analytics_engine.py.

Covers:
  - run_anomaly_detection: simple and advanced methods
  - run_clustering: DBSCAN geo-temporal clustering
  - Empty data handling
  - Error cases
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta


# ── Helpers ──────────────────────────────────────────────────────────


def _make_events_df(n: int = 100) -> pd.DataFrame:
    """Generate a realistic events DataFrame for testing."""
    rng = np.random.RandomState(42)
    base_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    data = {
        "ts": [base_time + timedelta(minutes=i) for i in range(n)],
        "user_id": rng.choice(["u1", "u2", "u3"], size=n),
        "event_type": rng.choice(["auth_success", "auth_failure", "login"], size=n),
        "ip": [f"10.0.0.{rng.randint(1, 255)}" for _ in range(n)],
        "device_id": rng.choice(["d1", "d2", "d3", "d4"], size=n),
        "geo_lat": rng.uniform(45.0, 50.0, size=n),
        "geo_lon": rng.uniform(0.0, 5.0, size=n),
        "country": rng.choice(["FR", "DE", "IT"], size=n),
    }
    return pd.DataFrame(data)


def _make_mock_engine(df: pd.DataFrame):
    """Build a mock execution engine that returns the given DataFrame."""
    from cloud.execution.base import ExecutionResult

    result = ExecutionResult(
        data=df,
        total_rows=len(df),
        columns=list(df.columns),
        engine="local",
        execution_time_ms=5.0,
    )

    mock_engine = MagicMock()
    mock_engine.execute_pipeline = AsyncMock(return_value=result)
    mock_engine.engine_name.return_value = "local"
    return mock_engine


# ── run_anomaly_detection: simple ────────────────────────────────────


@pytest.mark.asyncio
async def test_anomaly_simple_returns_results():
    from cloud.analytics_engine import run_anomaly_detection

    df = _make_events_df(100)
    mock_engine = _make_mock_engine(df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        result = await run_anomaly_detection(
            pipeline=[{"type": "source", "config": {"table": "test"}}],
            method="simple",
            z_thr=2.0,
            bucket="5m",
        )

    assert result["status"] == "ok"
    assert result["method"] == "simple"
    assert result["engine"] == "local"
    assert result["total_rows"] == 100
    assert "anomaly_count" in result
    assert "stats" in result
    assert result["stats"]["total_rows"] == 100
    assert "anomaly_rate" in result["stats"]
    assert result["execution_time_ms"] >= 0


@pytest.mark.asyncio
async def test_anomaly_simple_empty_data():
    from cloud.analytics_engine import run_anomaly_detection

    empty_df = pd.DataFrame()
    mock_engine = _make_mock_engine(empty_df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        result = await run_anomaly_detection(
            pipeline=[{"type": "source", "config": {"table": "empty"}}],
            method="simple",
        )

    assert result["status"] == "ok"
    assert result["total_rows"] == 0
    assert result["anomaly_count"] == 0
    assert result["data"] == []


@pytest.mark.asyncio
async def test_anomaly_simple_score_stats():
    from cloud.analytics_engine import run_anomaly_detection

    # Create data with enough rows for meaningful stats
    df = _make_events_df(200)
    mock_engine = _make_mock_engine(df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        result = await run_anomaly_detection(
            pipeline=[{"type": "source", "config": {"table": "test"}}],
            method="simple",
            z_thr=1.5,  # Lower threshold to get more anomalies
            bucket="1m",
        )

    stats = result["stats"]
    assert "total_rows" in stats
    assert "anomaly_count" in stats
    assert "anomaly_rate" in stats
    # Score stats may or may not be present depending on results
    if stats["anomaly_count"] > 0:
        assert len(result["anomalous_rows"]) > 0


# ── run_anomaly_detection: advanced ──────────────────────────────────


@pytest.mark.asyncio
async def test_anomaly_advanced_returns_results():
    from cloud.analytics_engine import run_anomaly_detection

    df = _make_events_df(100)
    mock_engine = _make_mock_engine(df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        result = await run_anomaly_detection(
            pipeline=[{"type": "source", "config": {"table": "test"}}],
            method="advanced",
            contamination=0.1,
        )

    assert result["status"] == "ok"
    assert result["method"] == "advanced"
    assert result["total_rows"] == 100
    assert "anomaly_count" in result
    assert "stats" in result


@pytest.mark.asyncio
async def test_anomaly_advanced_empty_data():
    from cloud.analytics_engine import run_anomaly_detection

    empty_df = pd.DataFrame()
    mock_engine = _make_mock_engine(empty_df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        result = await run_anomaly_detection(
            pipeline=[{"type": "source", "config": {"table": "empty"}}],
            method="advanced",
        )

    assert result["total_rows"] == 0
    assert result["anomaly_count"] == 0


# ── run_anomaly_detection: invalid method ────────────────────────────


@pytest.mark.asyncio
async def test_anomaly_invalid_method():
    from cloud.analytics_engine import run_anomaly_detection

    df = _make_events_df(10)
    mock_engine = _make_mock_engine(df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        with pytest.raises(ValueError, match="Unknown anomaly method"):
            await run_anomaly_detection(
                pipeline=[{"type": "source", "config": {"table": "test"}}],
                method="nonexistent_method",
            )


# ── run_anomaly_detection: serialization ─────────────────────────────


@pytest.mark.asyncio
async def test_anomaly_serializes_datetime():
    """Verify datetime values are converted to ISO strings in the response."""
    from cloud.analytics_engine import run_anomaly_detection

    df = _make_events_df(50)
    mock_engine = _make_mock_engine(df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        result = await run_anomaly_detection(
            pipeline=[{"type": "source", "config": {"table": "test"}}],
            method="simple",
            z_thr=1.0,  # Low threshold to likely get anomalies
        )

    # Check that any returned rows have serialized timestamps
    for row in result.get("anomalous_rows", []):
        if "ts" in row and row["ts"] is not None:
            assert isinstance(row["ts"], str), f"ts should be string, got {type(row['ts'])}"


# ── run_clustering ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clustering_returns_results():
    from cloud.analytics_engine import run_clustering

    df = _make_events_df(100)
    mock_engine = _make_mock_engine(df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        result = await run_clustering(
            pipeline=[{"type": "source", "config": {"table": "test"}}],
            eps=0.7,
            min_samples=5,
        )

    assert result["status"] == "ok"
    assert result["engine"] == "local"
    assert result["total_rows"] == 100
    assert "num_clusters" in result
    assert "noise_count" in result
    assert "cluster_stats" in result
    assert "data" in result
    assert result["execution_time_ms"] >= 0


@pytest.mark.asyncio
async def test_clustering_empty_data():
    from cloud.analytics_engine import run_clustering

    empty_df = pd.DataFrame()
    mock_engine = _make_mock_engine(empty_df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        result = await run_clustering(
            pipeline=[{"type": "source", "config": {"table": "empty"}}],
        )

    assert result["status"] == "ok"
    assert result["total_rows"] == 0
    assert result["num_clusters"] == 0
    assert result["data"] == []


@pytest.mark.asyncio
async def test_clustering_no_geo_data():
    """Clustering should handle data with no geo coordinates gracefully."""
    from cloud.analytics_engine import run_clustering

    df = pd.DataFrame({
        "ts": [datetime(2025, 1, 1, tzinfo=timezone.utc)] * 20,
        "user_id": ["u1"] * 20,
        "geo_lat": [None] * 20,
        "geo_lon": [None] * 20,
    })
    mock_engine = _make_mock_engine(df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        result = await run_clustering(
            pipeline=[{"type": "source", "config": {"table": "test"}}],
        )

    assert result["status"] == "ok"
    assert result["total_rows"] == 20
    # All points assigned to noise (-1) since no valid geo data
    assert result["num_clusters"] == 0


@pytest.mark.asyncio
async def test_clustering_finds_clusters():
    """With tight clusters, DBSCAN should find at least one."""
    from cloud.analytics_engine import run_clustering

    rng = np.random.RandomState(42)
    n = 60
    base_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Cluster 1: Paris area
    lats_1 = rng.normal(48.85, 0.01, size=30)
    lons_1 = rng.normal(2.35, 0.01, size=30)
    # Cluster 2: Berlin area
    lats_2 = rng.normal(52.52, 0.01, size=30)
    lons_2 = rng.normal(13.40, 0.01, size=30)

    df = pd.DataFrame({
        "ts": [base_time + timedelta(minutes=i) for i in range(n)],
        "user_id": ["u1"] * n,
        "geo_lat": list(lats_1) + list(lats_2),
        "geo_lon": list(lons_1) + list(lons_2),
    })
    mock_engine = _make_mock_engine(df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        result = await run_clustering(
            pipeline=[{"type": "source", "config": {"table": "test"}}],
            eps=0.5,
            min_samples=5,
        )

    assert result["status"] == "ok"
    assert result["total_rows"] == 60
    assert result["num_clusters"] >= 1

    # Check cluster stats have expected fields
    for stat in result["cluster_stats"]:
        assert "cluster_id" in stat
        assert "size" in stat
        assert stat["size"] > 0
        assert "centroid_lat" in stat
        assert "centroid_lon" in stat


@pytest.mark.asyncio
async def test_clustering_cluster_stats_have_time():
    """Cluster stats should include time_start and time_end."""
    from cloud.analytics_engine import run_clustering

    rng = np.random.RandomState(42)
    n = 30
    base_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    df = pd.DataFrame({
        "ts": [base_time + timedelta(minutes=i) for i in range(n)],
        "user_id": ["u1"] * n,
        "geo_lat": rng.normal(48.85, 0.005, size=n),
        "geo_lon": rng.normal(2.35, 0.005, size=n),
    })
    mock_engine = _make_mock_engine(df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        result = await run_clustering(
            pipeline=[{"type": "source", "config": {"table": "test"}}],
            eps=1.0,
            min_samples=3,
        )

    for stat in result["cluster_stats"]:
        if stat["size"] > 0:
            assert "time_start" in stat
            assert "time_end" in stat


@pytest.mark.asyncio
async def test_clustering_data_limited_to_5000():
    """Response data should be limited to 5000 rows."""
    from cloud.analytics_engine import run_clustering

    rng = np.random.RandomState(42)
    n = 6000
    base_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    df = pd.DataFrame({
        "ts": [base_time + timedelta(seconds=i) for i in range(n)],
        "user_id": ["u1"] * n,
        "geo_lat": rng.normal(48.85, 0.01, size=n),
        "geo_lon": rng.normal(2.35, 0.01, size=n),
    })
    mock_engine = _make_mock_engine(df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        result = await run_clustering(
            pipeline=[{"type": "source", "config": {"table": "test"}}],
            eps=1.0,
            min_samples=3,
        )

    assert result["total_rows"] == 6000
    assert len(result["data"]) <= 5000


@pytest.mark.asyncio
async def test_clustering_serializes_datetime():
    """Verify datetime values are converted to ISO strings in the response."""
    from cloud.analytics_engine import run_clustering

    df = _make_events_df(30)
    mock_engine = _make_mock_engine(df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine):
        result = await run_clustering(
            pipeline=[{"type": "source", "config": {"table": "test"}}],
            eps=1.0,
            min_samples=3,
        )

    for row in result["data"]:
        if "ts" in row and row["ts"] is not None:
            assert isinstance(row["ts"], str), f"ts should be string, got {type(row['ts'])}"


# ── Integration: engine wiring ───────────────────────────────────────


@pytest.mark.asyncio
async def test_anomaly_uses_execution_engine():
    """Verify that run_anomaly_detection calls get_engine and execute_pipeline."""
    from cloud.analytics_engine import run_anomaly_detection

    df = _make_events_df(50)
    mock_engine = _make_mock_engine(df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine) as mock_get:
        await run_anomaly_detection(
            pipeline=[{"type": "source", "config": {"table": "test"}}],
            method="simple",
        )

    mock_get.assert_called_once()
    mock_engine.execute_pipeline.assert_called_once()

    # Check pipeline was passed through
    call_args = mock_engine.execute_pipeline.call_args
    assert call_args[0][0] == [{"type": "source", "config": {"table": "test"}}]


@pytest.mark.asyncio
async def test_clustering_uses_execution_engine():
    """Verify that run_clustering calls get_engine and execute_pipeline."""
    from cloud.analytics_engine import run_clustering

    df = _make_events_df(50)
    mock_engine = _make_mock_engine(df)

    with patch("cloud.execution.engine_factory.get_engine", return_value=mock_engine) as mock_get:
        await run_clustering(
            pipeline=[
                {"type": "source", "config": {"table": "events"}},
                {"type": "filter", "config": {"conditions": [{"field": "country", "op": "eq", "value": "FR"}]}},
            ],
        )

    mock_get.assert_called_once()
    mock_engine.execute_pipeline.assert_called_once()

    call_args = mock_engine.execute_pipeline.call_args
    assert len(call_args[0][0]) == 2
    assert call_args[0][0][0]["type"] == "source"
    assert call_args[0][0][1]["type"] == "filter"