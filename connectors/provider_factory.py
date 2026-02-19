# connectors/provider_factory.py
"""
Factory for creating DataSourceProvider instances from cloud config.
"""
from __future__ import annotations

from typing import Optional

from connectors.base import DataSourceProvider
from cloud.config import (
    LizardCloudConfig,
    StorageConnectionConfig,
    DatabricksConnectionConfig,
)
from cloud.auth import create_auth_provider
from cloud.connectivity import EndpointResolver, GatewayRegistry


def create_storage_provider(
    conn: StorageConnectionConfig,
    config: LizardCloudConfig,
) -> DataSourceProvider:
    """Build an AzureStorageProvider from a StorageConnectionConfig."""
    from connectors.azure.storage import AzureStorageProvider

    registry = GatewayRegistry.from_config(config)
    resolver = EndpointResolver(registry)
    auth = create_auth_provider(conn.auth)

    return AzureStorageProvider(
        account_name=conn.account_name,
        auth_provider=auth,
        endpoint_resolver=resolver,
        container=conn.container,
        connectivity=conn.connectivity,
        gateway_name=conn.gateway_name,
        endpoint_type=conn.endpoint_type,
    )


def create_dbfs_provider(
    conn: DatabricksConnectionConfig,
    config: LizardCloudConfig,
) -> DataSourceProvider:
    """Build a DBFSProvider from a DatabricksConnectionConfig."""
    from connectors.azure.dbfs import DBFSProvider

    registry = GatewayRegistry.from_config(config)
    resolver = EndpointResolver(registry)
    auth = create_auth_provider(conn.auth)

    return DBFSProvider(
        workspace_id=conn.workspace_id,
        auth_provider=auth,
        endpoint_resolver=resolver,
        connectivity=conn.connectivity,
        gateway_name=conn.gateway_name,
    )