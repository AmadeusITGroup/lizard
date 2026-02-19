# connectors/azure/dbfs.py
"""
Databricks DBFS data-source provider.

Lists and reads files from DBFS via the Databricks REST API.
All Databricks SDK imports are lazy.
"""
from __future__ import annotations

import io
import base64
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from connectors.base import DataSourceInfo, DataSourceProvider
from cloud.auth import AuthProvider
from cloud.connectivity import EndpointResolver
from cloud.diagnostics import ConfigurationError, ConnectivityError

log = logging.getLogger("lizard.connectors.azure.dbfs")

# Maximum DBFS read chunk (1 MB via REST API)
_DBFS_READ_CHUNK = 1_048_576


def _detect_format(path: str) -> str:
    lower = path.lower()
    if lower.endswith(".parquet") or lower.endswith(".pq"):
        return "parquet"
    if lower.endswith(".json") or lower.endswith(".jsonl"):
        return "json"
    return "csv"


class DBFSProvider(DataSourceProvider):
    """
    Read data from Databricks DBFS.

    Uses the Databricks REST API (``/api/2.0/dbfs/...``) behind the scenes.
    """

    def __init__(
        self,
        workspace_id: str,
        auth_provider: AuthProvider,
        endpoint_resolver: EndpointResolver,
        connectivity: str = "direct",
        gateway_name: Optional[str] = None,
    ) -> None:
        self._workspace_id = workspace_id
        self._auth = auth_provider
        self._resolver = endpoint_resolver
        self._connectivity = connectivity
        self._gateway_name = gateway_name

    def _get_host(self) -> str:
        return self._resolver.resolve_databricks_host(
            workspace_id=self._workspace_id,
            connectivity=self._connectivity,
            gateway_name=self._gateway_name,
        )

    def _headers(self) -> Dict[str, str]:
        from cloud.constants import DATABRICKS_SCOPE

        token = self._auth.get_token([DATABRICKS_SCOPE])
        return {
            "Authorization": f"Bearer {token.token}",
            "Content-Type": "application/json",
        }

    def _api(self, endpoint: str) -> str:
        host = self._get_host().rstrip("/")
        return f"{host}/api/2.0{endpoint}"

    # ---- DataSourceProvider interface ------------------------------------

    async def list_sources(
        self,
        prefix: str = "",
        limit: int = 100,
        offset: int = 0,
        search: Optional[str] = None,
    ) -> List[DataSourceInfo]:
        import httpx

        dbfs_path = prefix or "/"
        url = self._api("/dbfs/list")

        try:
            async with httpx.AsyncClient(timeout=30, verify=False) as client:
                resp = await client.get(
                    url,
                    headers=self._headers(),
                    params={"path": dbfs_path},
                )
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            raise ConnectivityError(
                message=f"Failed to list DBFS path '{dbfs_path}': {exc}",
                action="Check Databricks workspace connectivity and credentials.",
                context={
                    "workspace_id": self._workspace_id,
                    "path": dbfs_path,
                    "error": str(exc),
                },
            ) from exc

        files = data.get("files", [])
        results: List[DataSourceInfo] = []

        for f in files:
            name = f.get("path", "").split("/")[-1]

            # Search filter
            if search and search.lower() not in name.lower():
                continue

            results.append(
                DataSourceInfo(
                    name=name,
                    path=f"dbfs:{f['path']}",
                    provider="dbfs",
                    size_bytes=f.get("file_size"),
                    last_modified=None,
                    content_type="directory" if f.get("is_dir") else None,
                    extra={
                        "is_dir": f.get("is_dir", False),
                        "modification_time": f.get("modification_time"),
                    },
                )
            )

        # Pagination
        return results[offset : offset + limit]

    async def read_dataset(
        self,
        path: str,
        format: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        import httpx

        dbfs_path = path.replace("dbfs:", "", 1) if path.startswith("dbfs:") else path
        fmt = format or _detect_format(dbfs_path)

        # Read file in chunks
        all_data = b""
        read_offset = 0

        try:
            async with httpx.AsyncClient(timeout=60, verify=False) as client:
                while True:
                    resp = await client.get(
                        self._api("/dbfs/read"),
                        headers=self._headers(),
                        params={
                            "path": dbfs_path,
                            "offset": read_offset,
                            "length": _DBFS_READ_CHUNK,
                        },
                    )
                    resp.raise_for_status()
                    result = resp.json()

                    chunk = base64.b64decode(result.get("data", ""))
                    all_data += chunk

                    bytes_read = result.get("bytes_read", 0)
                    if bytes_read < _DBFS_READ_CHUNK:
                        break
                    read_offset += bytes_read
        except Exception as exc:
            raise ConnectivityError(
                message=f"Failed to read DBFS file '{dbfs_path}': {exc}",
                action="Check that the file exists and credentials have access.",
                context={
                    "workspace_id": self._workspace_id,
                    "path": dbfs_path,
                    "error": str(exc),
                },
            ) from exc

        return self._bytes_to_dataframe(all_data, fmt, limit, **kwargs)

    async def dataset_exists(self, path: str) -> bool:
        import httpx

        dbfs_path = path.replace("dbfs:", "", 1) if path.startswith("dbfs:") else path

        try:
            async with httpx.AsyncClient(timeout=15, verify=False) as client:
                resp = await client.get(
                    self._api("/dbfs/get-status"),
                    headers=self._headers(),
                    params={"path": dbfs_path},
                )
                return resp.status_code == 200
        except Exception:
            return False

    def provider_name(self) -> str:
        return "dbfs"

    # ---- internal helpers ------------------------------------------------

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