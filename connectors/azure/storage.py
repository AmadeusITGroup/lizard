# connectors/azure/storage.py
"""
Azure Blob Storage / ADLS Gen2 data-source provider.

All Azure SDK imports are lazy — this module is safe to import even when
``azure-storage-blob`` is not installed.
"""
from __future__ import annotations

import io
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from connectors.base import DataSourceInfo, DataSourceProvider
from cloud.auth import AuthProvider, AccessToken
from cloud.connectivity import EndpointResolver
from cloud.diagnostics import ConfigurationError, ConnectivityError

log = logging.getLogger("lizard.connectors.azure.storage")


def _detect_format(path: str) -> str:
    """Guess file format from extension."""
    lower = path.lower()
    if lower.endswith(".parquet") or lower.endswith(".pq"):
        return "parquet"
    if lower.endswith(".json") or lower.endswith(".jsonl"):
        return "json"
    return "csv"


class AzureStorageProvider(DataSourceProvider):
    """
    Read data from Azure Blob Storage or ADLS Gen2.

    Supports:
      - Listing containers
      - Listing blobs (with prefix/search/pagination)
      - Reading CSV, Parquet, JSON blobs into DataFrames
    """

    def __init__(
        self,
        account_name: str,
        auth_provider: AuthProvider,
        endpoint_resolver: EndpointResolver,
        container: str = "",
        connectivity: str = "direct",
        gateway_name: Optional[str] = None,
        endpoint_type: str = "blob",
    ) -> None:
        self._account_name = account_name
        self._auth = auth_provider
        self._resolver = endpoint_resolver
        self._default_container = container
        self._connectivity = connectivity
        self._gateway_name = gateway_name
        self._endpoint_type = endpoint_type
        self._client: Any = None  # lazy

    def _get_base_url(self) -> str:
        return self._resolver.resolve_storage_url(
            account_name=self._account_name,
            connectivity=self._connectivity,
            gateway_name=self._gateway_name,
            endpoint_type=self._endpoint_type,
        )

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError:
            raise ConfigurationError(
                message="azure-storage-blob is not installed.",
                action="Install cloud dependencies: pip install lizard-fpv[cloud]",
                context={},
            )

        base_url = self._get_base_url()
        token = self._auth.get_token(["https://storage.azure.com/.default"])

        # Build credential wrapper for the SDK
        class _StaticTokenCredential:
            """Wraps our AccessToken so the Azure SDK can use it."""

            def __init__(self, access_token: AccessToken):
                self._token = access_token

            def get_token(self, *scopes, **kwargs):
                from azure.core.credentials import AccessToken as AzAccessToken
                return AzAccessToken(self._token.token, int(self._token.expires_on))

        self._client = BlobServiceClient(
            account_url=base_url,
            credential=_StaticTokenCredential(token),
        )
        return self._client

    # ---- DataSourceProvider interface ------------------------------------

    async def list_containers(self) -> List[Dict[str, Any]]:
        """List all containers in the storage account."""
        client = self._ensure_client()
        containers: List[Dict[str, Any]] = []
        try:
            for c in client.list_containers():
                containers.append({
                    "name": c.name,
                    "last_modified": c.last_modified.isoformat() if c.last_modified else None,
                    "metadata": dict(c.metadata) if c.metadata else {},
                })
        except Exception as exc:
            raise ConnectivityError(
                message=f"Failed to list containers on '{self._account_name}': {exc}",
                action="Check network connectivity and credentials.",
                context={"account": self._account_name, "error": str(exc)},
            ) from exc
        return containers

    async def list_sources(
        self,
        prefix: str = "",
        limit: int = 100,
        offset: int = 0,
        search: Optional[str] = None,
    ) -> List[DataSourceInfo]:
        container = prefix or self._default_container
        if not container:
            raise ConfigurationError(
                message="No container specified for listing blobs.",
                action="Provide a container name via 'prefix' or configure a default container.",
                context={"account": self._account_name},
            )

        client = self._ensure_client()
        container_client = client.get_container_client(container)

        results: List[DataSourceInfo] = []
        count = 0
        skipped = 0

        try:
            for blob in container_client.list_blobs():
                name = blob.name

                # Search filter
                if search and search.lower() not in name.lower():
                    continue

                # Pagination
                if skipped < offset:
                    skipped += 1
                    continue

                results.append(
                    DataSourceInfo(
                        name=name.split("/")[-1],
                        path=f"abfss://{container}@{self._account_name}.dfs.core.windows.net/{name}"
                        if self._endpoint_type == "dfs"
                        else f"wasbs://{container}@{self._account_name}.blob.core.windows.net/{name}",
                        provider=f"azure_{self._endpoint_type}",
                        size_bytes=blob.size,
                        last_modified=blob.last_modified.isoformat() if blob.last_modified else None,
                        content_type=blob.content_settings.content_type if blob.content_settings else None,
                        container=container,
                        account=self._account_name,
                    )
                )
                count += 1
                if count >= limit:
                    break
        except Exception as exc:
            raise ConnectivityError(
                message=f"Failed to list blobs in '{container}': {exc}",
                action="Check container name, credentials, and network connectivity.",
                context={
                    "account": self._account_name,
                    "container": container,
                    "error": str(exc),
                },
            ) from exc

        return results

    async def read_dataset(
        self,
        path: str,
        format: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        # Parse path to extract container + blob name
        container, blob_name = self._parse_path(path)
        fmt = format or _detect_format(blob_name)

        client = self._ensure_client()
        blob_client = client.get_blob_client(container=container, blob=blob_name)

        try:
            stream = blob_client.download_blob()
            data = stream.readall()
        except Exception as exc:
            raise ConnectivityError(
                message=f"Failed to read blob '{blob_name}' from container '{container}': {exc}",
                action="Check that the blob exists and credentials have Data Reader role.",
                context={
                    "account": self._account_name,
                    "container": container,
                    "blob": blob_name,
                    "error": str(exc),
                },
            ) from exc

        return self._bytes_to_dataframe(data, fmt, limit, **kwargs)

    async def dataset_exists(self, path: str) -> bool:
        container, blob_name = self._parse_path(path)
        client = self._ensure_client()
        blob_client = client.get_blob_client(container=container, blob=blob_name)
        try:
            blob_client.get_blob_properties()
            return True
        except Exception:
            return False

    def provider_name(self) -> str:
        return f"azure_{self._endpoint_type}"

    # ---- internal helpers ------------------------------------------------

    def _parse_path(self, path: str) -> tuple:
        """
        Parse a path into (container, blob_name).

        Accepts:
          abfss://container@account.dfs.core.windows.net/path/to/file
          wasbs://container@account.blob.core.windows.net/path/to/file
          container/path/to/file
          path/to/file  (uses default container)
        """
        for scheme in ("abfss://", "wasbs://", "abfs://", "wasb://"):
            if path.startswith(scheme):
                rest = path[len(scheme):]
                container_part, blob_part = rest.split("/", 1) if "/" in rest else (rest, "")
                container = container_part.split("@")[0]
                # blob_part may contain the account domain prefix — strip it
                if ".core.windows.net/" in blob_part:
                    blob_part = blob_part.split(".core.windows.net/", 1)[1]
                return container, blob_part

        # Plain path: container/blob or just blob
        if "/" in path:
            parts = path.split("/", 1)
            return parts[0], parts[1]

        # Single name — assume default container
        return self._default_container, path

    @staticmethod
    def _bytes_to_dataframe(
        data: bytes,
        fmt: str,
        limit: Optional[int],
        **kwargs: Any,
    ) -> pd.DataFrame:
        if fmt == "parquet":
            df = pd.read_parquet(io.BytesIO(data), **kwargs)
        elif fmt == "json":
            df = pd.read_json(io.BytesIO(data), lines=True, **kwargs)
        else:
            df = pd.read_csv(io.BytesIO(data), **kwargs)

        if limit is not None and len(df) > limit:
            df = df.head(limit)
        return df