# cloud/output_storage.py
"""
Cloud output storage — write pipeline results to Azure Blob Storage or DBFS.

Supports:
  - CSV export
  - Parquet export
  - Writing to Azure Blob Storage containers
  - Writing to Databricks DBFS
"""
from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
import structlog

from cloud.config import get_config
from cloud.diagnostics import ConfigurationError, ConnectivityError

log = structlog.get_logger(__name__)


def _generate_filename(prefix: str, format: str) -> str:
    """Generate a timestamped filename for export."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    ext = "csv" if format == "csv" else "parquet"
    return f"{prefix}_{ts}.{ext}"


def _df_to_bytes(df: pd.DataFrame, format: str) -> bytes:
    """Serialize a DataFrame to bytes in the given format."""
    if format == "csv":
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        return buf.getvalue().encode("utf-8")
    elif format == "parquet":
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        return buf.getvalue()
    else:
        raise ValueError(f"Unsupported format: {format}. Use 'csv' or 'parquet'.")


async def export_to_blob_storage(
    df: pd.DataFrame,
    connection_name: str,
    container: Optional[str] = None,
    path_prefix: str = "lizard-exports",
    filename: Optional[str] = None,
    format: str = "parquet",
) -> Dict[str, Any]:
    """
    Export a DataFrame to Azure Blob Storage.

    Args:
        df: The DataFrame to export.
        connection_name: Name of the storage connection from cloud config.
        container: Target container (overrides connection default).
        path_prefix: Path prefix within the container.
        filename: Custom filename (auto-generated if None).
        format: 'csv' or 'parquet'.

    Returns:
        Dict with export metadata (path, size, rows, etc.)
    """
    cfg = get_config()
    conn = cfg.get_storage_connection(connection_name)
    if conn is None:
        raise ConfigurationError(
            message=f"Storage connection '{connection_name}' not found.",
            action="Add it in Cloud Settings → Storage Connections.",
            context={"connection_name": connection_name},
        )

    target_container = container or conn.container
    if not target_container:
        raise ConfigurationError(
            message="No container specified for blob export.",
            action="Provide a container name or set one in the storage connection.",
            context={"connection_name": connection_name},
        )

    # Generate filename
    if not filename:
        filename = _generate_filename("export", format)
    full_path = f"{path_prefix}/{filename}" if path_prefix else filename

    # Serialize
    data_bytes = _df_to_bytes(df, format)

    # Upload via provider
    try:
        from connectors.provider_factory import create_storage_provider

        provider = create_storage_provider(conn, cfg)
        await provider.write_bytes(
            container=target_container,
            path=full_path,
            data=data_bytes,
            content_type="text/csv" if format == "csv" else "application/octet-stream",
        )
    except AttributeError:
        # Provider doesn't have write_bytes yet — use raw SDK
        await _upload_blob_raw(conn, cfg, target_container, full_path, data_bytes)

    log.info(
        "exported_to_blob",
        connection=connection_name,
        container=target_container,
        path=full_path,
        format=format,
        rows=len(df),
        size_bytes=len(data_bytes),
    )

    return {
        "status": "ok",
        "connection": connection_name,
        "container": target_container,
        "path": full_path,
        "filename": filename,
        "format": format,
        "rows": len(df),
        "columns": list(df.columns),
        "size_bytes": len(data_bytes),
    }


async def export_to_dbfs(
    df: pd.DataFrame,
    connection_name: str,
    path_prefix: str = "/lizard-exports",
    filename: Optional[str] = None,
    format: str = "parquet",
) -> Dict[str, Any]:
    """
    Export a DataFrame to Databricks DBFS.

    Args:
        df: The DataFrame to export.
        connection_name: Name of the Databricks connection from cloud config.
        path_prefix: DBFS path prefix.
        filename: Custom filename (auto-generated if None).
        format: 'csv' or 'parquet'.

    Returns:
        Dict with export metadata.
    """
    cfg = get_config()
    conn = cfg.get_databricks_connection(connection_name)
    if conn is None:
        raise ConfigurationError(
            message=f"Databricks connection '{connection_name}' not found.",
            action="Add it in Cloud Settings → Databricks Connections.",
            context={"connection_name": connection_name},
        )

    if not filename:
        filename = _generate_filename("export", format)
    full_path = f"{path_prefix.rstrip('/')}/{filename}"

    data_bytes = _df_to_bytes(df, format)

    try:
        await _upload_dbfs_raw(conn, cfg, full_path, data_bytes)
    except Exception as exc:
        raise ConnectivityError(
            message=f"Failed to write to DBFS: {exc}",
            action="Check Databricks connection and DBFS permissions.",
            context={"connection_name": connection_name, "path": full_path},
        )

    log.info(
        "exported_to_dbfs",
        connection=connection_name,
        path=full_path,
        format=format,
        rows=len(df),
        size_bytes=len(data_bytes),
    )

    return {
        "status": "ok",
        "connection": connection_name,
        "path": full_path,
        "filename": filename,
        "format": format,
        "rows": len(df),
        "columns": list(df.columns),
        "size_bytes": len(data_bytes),
    }


async def _upload_blob_raw(conn, cfg, container: str, path: str, data: bytes) -> None:
    """Fallback: upload bytes to Azure Blob using the SDK directly."""
    from cloud.auth import create_auth_provider
    from cloud.connectivity import GatewayRegistry, EndpointResolver

    registry = GatewayRegistry.from_config(cfg)
    resolver = EndpointResolver(registry)
    base_url = resolver.resolve_storage_url(
        conn.account_name, conn.connectivity, conn.gateway_name, conn.endpoint_type
    )

    auth = create_auth_provider(conn.auth)
    token = auth.get_token(["https://storage.azure.com/.default"])

    import httpx

    url = f"{base_url}/{container}/{path}"
    headers = {
        "Authorization": f"Bearer {token.token}",
        "x-ms-blob-type": "BlockBlob",
        "x-ms-version": "2021-08-06",
    }

    async with httpx.AsyncClient(timeout=120, verify=False) as client:
        resp = await client.put(url, content=data, headers=headers)
        if resp.status_code >= 400:
            raise ConnectivityError(
                message=f"Blob upload failed: HTTP {resp.status_code}",
                action="Check storage permissions and container existence.",
                context={"url": url, "status": resp.status_code},
            )


async def _upload_dbfs_raw(conn, cfg, path: str, data: bytes) -> None:
    """Upload bytes to DBFS using the Databricks REST API."""
    from cloud.auth import create_auth_provider
    from cloud.connectivity import GatewayRegistry, EndpointResolver
    import base64

    registry = GatewayRegistry.from_config(cfg)
    resolver = EndpointResolver(registry)
    host = resolver.resolve_databricks_host(
        conn.workspace_id, conn.connectivity, conn.gateway_name
    )

    auth = create_auth_provider(conn.auth)
    from cloud.constants import DATABRICKS_SCOPE
    token = auth.get_token([DATABRICKS_SCOPE])

    import httpx

    headers = {"Authorization": f"Bearer {token.token}"}

    async with httpx.AsyncClient(timeout=120, verify=False) as client:
        # Use the DBFS put API (base64 for files < 1MB, streaming for larger)
        if len(data) <= 1_048_576:
            resp = await client.post(
                f"{host}/api/2.0/dbfs/put",
                json={
                    "path": path,
                    "contents": base64.b64encode(data).decode("ascii"),
                    "overwrite": True,
                },
                headers=headers,
            )
        else:
            # Multi-part upload for larger files
            # Step 1: Create
            resp = await client.post(
                f"{host}/api/2.0/dbfs/create",
                json={"path": path, "overwrite": True},
                headers=headers,
            )
            if resp.status_code >= 400:
                raise ConnectivityError(
                    message=f"DBFS create failed: HTTP {resp.status_code}",
                    context={"path": path},
                )
            handle = resp.json().get("handle")

            # Step 2: Add blocks (1MB chunks)
            chunk_size = 1_048_576
            for i in range(0, len(data), chunk_size):
                chunk = data[i: i + chunk_size]
                resp = await client.post(
                    f"{host}/api/2.0/dbfs/add-block",
                    json={
                        "handle": handle,
                        "data": base64.b64encode(chunk).decode("ascii"),
                    },
                    headers=headers,
                )
                if resp.status_code >= 400:
                    raise ConnectivityError(
                        message=f"DBFS add-block failed: HTTP {resp.status_code}",
                        context={"path": path, "chunk": i},
                    )

            # Step 3: Close
            resp = await client.post(
                f"{host}/api/2.0/dbfs/close",
                json={"handle": handle},
                headers=headers,
            )

        if resp.status_code >= 400:
            raise ConnectivityError(
                message=f"DBFS upload failed: HTTP {resp.status_code}",
                action="Check Databricks connection and DBFS permissions.",
                context={"path": path, "status": resp.status_code},
            )