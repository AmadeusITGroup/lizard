# tests/test_cluster_manager.py
"""
Tests for cloud/cluster_manager.py.

All Databricks SDK calls are mocked — no real cluster or network needed.
"""
from __future__ import annotations

import builtins
from unittest.mock import MagicMock, patch
import pytest

from cloud.diagnostics import ConfigurationError, ConnectivityError


# ── Helper: mock config with a Databricks connection ─────────────────

def _mock_config_with_connection(conn_name="test_conn", workspace_id="ws-123"):
    """Return a mocked LizardCloudConfig with one Databricks connection."""
    from cloud.config import (
        LizardCloudConfig,
        DatabricksConnectionConfig,
        AuthConfig,
        ClusterConfig,
    )

    conn = DatabricksConnectionConfig(
        name=conn_name,
        workspace_id=workspace_id,
        connectivity="direct",
        auth=AuthConfig(type="service_principal", client_id="cid", client_secret="csec", tenant_id="tid"),
        cluster=ClusterConfig(cluster_id="cluster-abc"),
    )
    cfg = LizardCloudConfig(
        mode="cloud",
        databricks_connections=[conn],
    )
    return cfg


def _import_blocker(blocked_module: str):
    """
    Return a side_effect function for patching builtins.__import__
    that blocks a specific module while allowing all others.
    """
    _real_import = builtins.__import__

    def _blocked_import(name, *args, **kwargs):
        if name == blocked_module or name.startswith(blocked_module + "."):
            raise ImportError(f"Mocked: {name} is not installed")
        return _real_import(name, *args, **kwargs)

    return _blocked_import


# ── _get_workspace_client ────────────────────────────────────────────

def test_get_workspace_client_missing_connection():
    """Raises ConfigurationError when connection name not found."""
    from cloud.cluster_manager import _get_workspace_client
    from cloud.config import LizardCloudConfig

    with patch("cloud.cluster_manager.get_config") as mock_get:
        mock_get.return_value = LizardCloudConfig()  # No connections
        with pytest.raises(ConfigurationError, match="not found"):
            _get_workspace_client("nonexistent")


def test_get_workspace_client_no_sdk():
    """Raises ConfigurationError when databricks-sdk is not installed."""
    from cloud.cluster_manager import _get_workspace_client

    cfg = _mock_config_with_connection()

    with patch("cloud.cluster_manager.get_config", return_value=cfg):
        with patch("cloud.cluster_manager.GatewayRegistry") as mock_gw_cls:
            mock_gw_cls.from_config.return_value = MagicMock()

            with patch("cloud.cluster_manager.EndpointResolver") as mock_resolver_cls:
                mock_resolver = MagicMock()
                mock_resolver.resolve_databricks_host.return_value = "https://test.databricks.com"
                mock_resolver_cls.return_value = mock_resolver

                # Simulate missing SDK by making the import raise ImportError
                with patch("builtins.__import__", side_effect=_import_blocker("databricks.sdk")):
                    with pytest.raises((ConfigurationError, ImportError)):
                        _get_workspace_client("test_conn")


# ── list_clusters ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_list_clusters_success():
    """list_clusters returns cluster info dicts from the SDK."""
    from cloud.cluster_manager import list_clusters

    mock_cluster = MagicMock()
    mock_cluster.cluster_id = "cluster-001"
    mock_cluster.cluster_name = "Analytics Cluster"
    mock_cluster.state = "RUNNING"
    mock_cluster.spark_version = "13.3.x-scala2.12"
    mock_cluster.node_type_id = "Standard_DS3_v2"
    mock_cluster.num_workers = 4
    mock_cluster.autotermination_minutes = 30
    mock_cluster.creator_user_name = "user@corp.com"

    mock_client = MagicMock()
    mock_client.clusters.list.return_value = [mock_cluster]

    with patch("cloud.cluster_manager._get_workspace_client", return_value=mock_client):
        result = await list_clusters("test_conn")

    assert len(result) == 1
    assert result[0]["cluster_id"] == "cluster-001"
    assert result[0]["cluster_name"] == "Analytics Cluster"
    assert result[0]["state"] == "RUNNING"
    assert result[0]["num_workers"] == 4


@pytest.mark.asyncio
async def test_list_clusters_empty():
    """list_clusters returns empty list when no clusters exist."""
    from cloud.cluster_manager import list_clusters

    mock_client = MagicMock()
    mock_client.clusters.list.return_value = []

    with patch("cloud.cluster_manager._get_workspace_client", return_value=mock_client):
        result = await list_clusters("test_conn")

    assert result == []


@pytest.mark.asyncio
async def test_list_clusters_sdk_error():
    """list_clusters raises ConnectivityError on SDK failure."""
    from cloud.cluster_manager import list_clusters

    mock_client = MagicMock()
    mock_client.clusters.list.side_effect = Exception("Network timeout")

    with patch("cloud.cluster_manager._get_workspace_client", return_value=mock_client):
        with pytest.raises(ConnectivityError, match="Failed to list clusters"):
            await list_clusters("test_conn")


# ── get_cluster_status ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_cluster_status_success():
    """get_cluster_status returns detailed cluster info."""
    from cloud.cluster_manager import get_cluster_status

    mock_cluster = MagicMock()
    mock_cluster.cluster_id = "cluster-001"
    mock_cluster.cluster_name = "My Cluster"
    mock_cluster.state = "RUNNING"
    mock_cluster.state_message = "Running since 10:00"
    mock_cluster.spark_version = "14.0.x-scala2.12"
    mock_cluster.node_type_id = "Standard_DS4_v2"
    mock_cluster.driver_node_type_id = "Standard_DS4_v2"
    mock_cluster.num_workers = 8
    mock_cluster.autoscale = None
    mock_cluster.autotermination_minutes = 60
    mock_cluster.spark_conf = {"spark.sql.shuffle.partitions": "200"}
    mock_cluster.creator_user_name = "admin@corp.com"
    mock_cluster.start_time = 1700000000000
    mock_cluster.last_activity_time = 1700003600000

    mock_client = MagicMock()
    mock_client.clusters.get.return_value = mock_cluster

    with patch("cloud.cluster_manager._get_workspace_client", return_value=mock_client):
        result = await get_cluster_status("test_conn", "cluster-001")

    assert result["cluster_id"] == "cluster-001"
    assert result["state"] == "RUNNING"
    assert result["num_workers"] == 8
    assert result["autoscale"] is None
    assert result["spark_conf"] == {"spark.sql.shuffle.partitions": "200"}


@pytest.mark.asyncio
async def test_get_cluster_status_with_autoscale():
    """get_cluster_status handles autoscale config."""
    from cloud.cluster_manager import get_cluster_status

    mock_autoscale = MagicMock()
    mock_autoscale.min_workers = 2
    mock_autoscale.max_workers = 10

    mock_cluster = MagicMock()
    mock_cluster.cluster_id = "cluster-002"
    mock_cluster.cluster_name = "Autoscale Cluster"
    mock_cluster.state = "RUNNING"
    mock_cluster.state_message = ""
    mock_cluster.spark_version = "14.0"
    mock_cluster.node_type_id = "Standard_DS3_v2"
    mock_cluster.driver_node_type_id = "Standard_DS3_v2"
    mock_cluster.num_workers = None
    mock_cluster.autoscale = mock_autoscale
    mock_cluster.autotermination_minutes = 45
    mock_cluster.spark_conf = None
    mock_cluster.creator_user_name = "user@corp.com"
    mock_cluster.start_time = None
    mock_cluster.last_activity_time = None

    mock_client = MagicMock()
    mock_client.clusters.get.return_value = mock_cluster

    with patch("cloud.cluster_manager._get_workspace_client", return_value=mock_client):
        result = await get_cluster_status("test_conn", "cluster-002")

    assert result["autoscale"]["min_workers"] == 2
    assert result["autoscale"]["max_workers"] == 10


@pytest.mark.asyncio
async def test_get_cluster_status_sdk_error():
    """get_cluster_status raises ConnectivityError on SDK failure."""
    from cloud.cluster_manager import get_cluster_status

    mock_client = MagicMock()
    mock_client.clusters.get.side_effect = Exception("Forbidden")

    with patch("cloud.cluster_manager._get_workspace_client", return_value=mock_client):
        with pytest.raises(ConnectivityError, match="Failed to get cluster status"):
            await get_cluster_status("test_conn", "bad-id")


# ── start_cluster ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_start_cluster_success():
    """start_cluster calls SDK and returns confirmation."""
    from cloud.cluster_manager import start_cluster

    mock_client = MagicMock()
    mock_client.clusters.start.return_value = None  # SDK returns None on success

    with patch("cloud.cluster_manager._get_workspace_client", return_value=mock_client):
        result = await start_cluster("test_conn", "cluster-001")

    assert result["status"] == "start_requested"
    assert result["cluster_id"] == "cluster-001"
    mock_client.clusters.start.assert_called_once_with("cluster-001")


@pytest.mark.asyncio
async def test_start_cluster_error():
    """start_cluster raises ConnectivityError on failure."""
    from cloud.cluster_manager import start_cluster

    mock_client = MagicMock()
    mock_client.clusters.start.side_effect = Exception("Permission denied")

    with patch("cloud.cluster_manager._get_workspace_client", return_value=mock_client):
        with pytest.raises(ConnectivityError, match="Failed to start cluster"):
            await start_cluster("test_conn", "cluster-001")


# ── stop_cluster ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_stop_cluster_success():
    """stop_cluster calls SDK and returns confirmation."""
    from cloud.cluster_manager import stop_cluster

    mock_client = MagicMock()
    mock_client.clusters.delete.return_value = None

    with patch("cloud.cluster_manager._get_workspace_client", return_value=mock_client):
        result = await stop_cluster("test_conn", "cluster-001")

    assert result["status"] == "stop_requested"
    assert result["cluster_id"] == "cluster-001"
    mock_client.clusters.delete.assert_called_once_with("cluster-001")


@pytest.mark.asyncio
async def test_stop_cluster_error():
    """stop_cluster raises ConnectivityError on failure."""
    from cloud.cluster_manager import stop_cluster

    mock_client = MagicMock()
    mock_client.clusters.delete.side_effect = Exception("Cannot stop")

    with patch("cloud.cluster_manager._get_workspace_client", return_value=mock_client):
        with pytest.raises(ConnectivityError, match="Failed to stop cluster"):
            await stop_cluster("test_conn", "cluster-001")


# ── list_warehouses ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_list_warehouses_success():
    """list_warehouses returns warehouse info dicts from the SDK."""
    from cloud.cluster_manager import list_warehouses

    mock_wh = MagicMock()
    mock_wh.id = "wh-001"
    mock_wh.name = "Serverless Warehouse"
    mock_wh.state = "RUNNING"
    mock_wh.cluster_size = "Medium"
    mock_wh.num_clusters = 1
    mock_wh.auto_stop_mins = 15
    mock_wh.warehouse_type = "PRO"
    mock_wh.creator_name = "admin@corp.com"

    mock_client = MagicMock()
    mock_client.warehouses.list.return_value = [mock_wh]

    with patch("cloud.cluster_manager._get_workspace_client", return_value=mock_client):
        result = await list_warehouses("test_conn")

    assert len(result) == 1
    assert result[0]["warehouse_id"] == "wh-001"
    assert result[0]["name"] == "Serverless Warehouse"
    assert result[0]["state"] == "RUNNING"


@pytest.mark.asyncio
async def test_list_warehouses_sdk_error():
    """list_warehouses raises ConnectivityError on SDK failure."""
    from cloud.cluster_manager import list_warehouses

    mock_client = MagicMock()
    mock_client.warehouses.list.side_effect = Exception("Unauthorized")

    with patch("cloud.cluster_manager._get_workspace_client", return_value=mock_client):
        with pytest.raises(ConnectivityError, match="Failed to list warehouses"):
            await list_warehouses("test_conn")