# connectors/local_provider.py
"""Local data source provider implementation."""
from __future__ import annotations

from typing import Any, Callable, List, Optional

import pandas as pd

from connectors.base import DataSourceInfo, DataSourceProvider


class LocalDataSourceProvider(DataSourceProvider):
    """Provider for local data sources (SQLite tables, local CSV files)."""

    def __init__(
        self,
        list_sources_func: Optional[Callable[[], Any]] = None,
        get_source_data_func: Optional[Callable[[str], Any]] = None,
    ):
        self._list_sources_func = list_sources_func
        self._get_source_data_func = get_source_data_func

    def provider_name(self) -> str:
        return "local"

    async def list_sources(self, prefix: str = "", limit: int = 100, offset: int = 0, search: Optional[str] = None) -> List[DataSourceInfo]:
        if self._list_sources_func is None:
            return []
        raw_sources = await self._list_sources_func()
        if search:
            raw_sources = [s for s in raw_sources if search.lower() in s.get("name", "").lower()]
        paginated = raw_sources[offset:offset + limit]
        return [
            DataSourceInfo(
                name=s.get("name", ""),
                path=s.get("path", s.get("name", "")),
                provider="local",
                size_bytes=s.get("size_bytes"),
                last_modified=s.get("last_modified"),
                extra={k: v for k, v in s.items() if k not in ("name", "path", "size_bytes", "last_modified")},
            )
            for s in paginated
        ]

    async def read_dataset(self, path: str, format: Optional[str] = None, limit: Optional[int] = None, **kwargs: Any) -> pd.DataFrame:
        if self._get_source_data_func is None:
            return pd.DataFrame()
        df = await self._get_source_data_func(path)
        if limit is not None and len(df) > limit:
            df = df.head(limit)
        return df

    async def dataset_exists(self, path: str) -> bool:
        if self._get_source_data_func is None:
            return False
        df = await self._get_source_data_func(path)
        return not df.empty