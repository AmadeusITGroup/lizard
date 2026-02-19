# cloud/cluster_manager.py
"""
Databricks cluster management utilities.

Provides listing, status checking, start/stop operations for Databricks
clusters via the SDK. Used by the cloud API and the SparkDatabricksEngine.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import structlog

from cloud.config import get_config, DatabricksConnectionConfig
from cloud.connectivity import GatewayRegistry, EndpointResolver
from cloud.diagnostics import ConfigurationError, ConnectivityError

log = structlog.get_logger(__name__)


def _get_workspace_client(connection_name: str):
    """
    Build a Databricks WorkspaceClient for the given connection name.
    Raises ConfigurationError if connection not found.
    """
    cfg = get_config()
    conn = cfg.get_databricks_connection(connection_name)
    if conn is None:
        raise ConfigurationError(
            message=f"Databricks connection '{connection_name}' not found.",
            action="Add it in Cloud Settings → Databricks Connections.",
            context={"connection_name": connection_name},
        )

    # Resolve host
    registry = GatewayRegistry.from_config(cfg)
    resolver = EndpointResolver(registry)
    host = resolver.resolve_databricks_host(
        conn.workspace_id, conn.connectivity, conn.gateway_name
    )

    auth_kwargs: Dict[str, Any] = {"host": host}
    if conn.auth and conn.auth.type == "service_principal":
        auth_kwargs["client_id"] = conn.auth.client_id
        auth_kwargs["client_secret"] = conn.auth.client_secret
        if conn.auth.tenant_id:
            auth_kwargs["azure_tenant_id"] = conn.auth.tenant_id

    try:
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient(**auth_kwargs)
    except ImportError:
        raise ConfigurationError(
            message="databricks-sdk is not installed.",
            action="Install it with: pip install 'lizard-fpv[cloud]'",
        )


async def list_clusters(connection_name: str) -> List[Dict[str, Any]]:
    """List all clusters available on the Databricks workspace."""
    try:
        client = _get_workspace_client(connection_name)
        clusters = client.clusters.list()

        result = []
        for c in clusters:
            result.append({
                "cluster_id": c.cluster_id,
                "cluster_name": c.cluster_name,
                "state": str(c.state),
                "spark_version": getattr(c, "spark_version", None),
                "node_type_id": getattr(c, "node_type_id", None),
                "num_workers": getattr(c, "num_workers", None),
                "autotermination_minutes": getattr(c, "autotermination_minutes", None),
                "creator_user_name": getattr(c, "creator_user_name", None),
            })
        return result

    except (ConfigurationError, ConnectivityError):
        raise
    except Exception as exc:
        raise ConnectivityError(
            message=f"Failed to list clusters: {exc}",
            action="Check your Databricks connection, credentials, and network/gateway.",
            context={"connection_name": connection_name},
        )


async def get_cluster_status(connection_name: str, cluster_id: str) -> Dict[str, Any]:
    """Get detailed status for a specific cluster."""
    try:
        client = _get_workspace_client(connection_name)
        c = client.clusters.get(cluster_id)

        return {
            "cluster_id": c.cluster_id,
            "cluster_name": c.cluster_name,
            "state": str(c.state),
            "state_message": getattr(c, "state_message", None),
            "spark_version": getattr(c, "spark_version", None),
            "node_type_id": getattr(c, "node_type_id", None),
            "driver_node_type_id": getattr(c, "driver_node_type_id", None),
            "num_workers": getattr(c, "num_workers", None),
            "autoscale": {
                "min_workers": getattr(getattr(c, "autoscale", None), "min_workers", None),
                "max_workers": getattr(getattr(c, "autoscale", None), "max_workers", None),
            } if getattr(c, "autoscale", None) else None,
            "autotermination_minutes": getattr(c, "autotermination_minutes", None),
            "spark_conf": dict(c.spark_conf) if getattr(c, "spark_conf", None) else {},
            "creator_user_name": getattr(c, "creator_user_name", None),
            "start_time": getattr(c, "start_time", None),
            "last_activity_time": getattr(c, "last_activity_time", None),
        }

    except (ConfigurationError, ConnectivityError):
        raise
    except Exception as exc:
        raise ConnectivityError(
            message=f"Failed to get cluster status: {exc}",
            action="Verify the cluster_id and connection settings.",
            context={"connection_name": connection_name, "cluster_id": cluster_id},
        )


async def start_cluster(connection_name: str, cluster_id: str) -> Dict[str, str]:
    """Start a terminated/stopped cluster."""
    try:
        client = _get_workspace_client(connection_name)
        client.clusters.start(cluster_id)
        log.info("cluster_start_requested", connection=connection_name, cluster_id=cluster_id)
        return {"status": "start_requested", "cluster_id": cluster_id}
    except Exception as exc:
        raise ConnectivityError(
            message=f"Failed to start cluster: {exc}",
            action="Check permissions — you need 'Can Restart' on this cluster.",
            context={"connection_name": connection_name, "cluster_id": cluster_id},
        )


async def stop_cluster(connection_name: str, cluster_id: str) -> Dict[str, str]:
    """Stop a running cluster (graceful termination)."""
    try:
        client = _get_workspace_client(connection_name)
        client.clusters.delete(cluster_id)  # SDK uses "delete" for stop
        log.info("cluster_stop_requested", connection=connection_name, cluster_id=cluster_id)
        return {"status": "stop_requested", "cluster_id": cluster_id}
    except Exception as exc:
        raise ConnectivityError(
            message=f"Failed to stop cluster: {exc}",
            action="Check permissions — you need 'Can Restart' on this cluster.",
            context={"connection_name": connection_name, "cluster_id": cluster_id},
        )


async def list_warehouses(connection_name: str) -> List[Dict[str, Any]]:
    """List all SQL Warehouses on the Databricks workspace."""
    try:
        client = _get_workspace_client(connection_name)
        warehouses = client.warehouses.list()

        result = []
        for w in warehouses:
            result.append({
                "warehouse_id": w.id,
                "name": w.name,
                "state": str(w.state),
                "cluster_size": getattr(w, "cluster_size", None),
                "num_clusters": getattr(w, "num_clusters", None),
                "auto_stop_mins": getattr(w, "auto_stop_mins", None),
                "warehouse_type": str(getattr(w, "warehouse_type", None)),
                "creator_name": getattr(w, "creator_name", None),
            })
        return result

    except (ConfigurationError, ConnectivityError):
        raise
    except Exception as exc:
        raise ConnectivityError(
            message=f"Failed to list warehouses: {exc}",
            action="Check your Databricks connection and permissions.",
            context={"connection_name": connection_name},
        )