# connectors/base.py
"""
Abstract base class for all LIZARD data-source providers.

Every provider must implement listing available datasets and reading a
dataset into a pandas DataFrame.  This ensures the rest of the system
(workbench, dashboard, anomaly detection) stays data-source agnostic.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class DataSourceInfo:
    """Metadata about one discoverable dataset (file, table, blob, DBFS path)."""

    name: str                       # display name (e.g. "fraud-2025.csv")
    path: str                       # full qualified path / URI
    provider: str                   # "local", "azure_blob", "azure_dfs", "dbfs"
    size_bytes: Optional[int] = None
    last_modified: Optional[str] = None
    content_type: Optional[str] = None
    container: Optional[str] = None      # Azure container name
    account: Optional[str] = None        # Azure storage account
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "name": self.name,
            "path": self.path,
            "provider": self.provider,
        }
        if self.size_bytes is not None:
            d["size_bytes"] = self.size_bytes
        if self.last_modified is not None:
            d["last_modified"] = self.last_modified
        if self.content_type is not None:
            d["content_type"] = self.content_type
        if self.container:
            d["container"] = self.container
        if self.account:
            d["account"] = self.account
        if self.extra:
            d["extra"] = self.extra
        return d


class DataSourceProvider(ABC):
    """
    Interface for data-source providers.

    Implementations:
      - LocalDataSourceProvider  (wraps existing SQLite / local CSV)
      - AzureStorageProvider     (Azure Blob + ADLS Gen2)
      - DBFSProvider             (Databricks DBFS)
    """

    @abstractmethod
    async def list_sources(
        self,
        prefix: str = "",
        limit: int = 100,
        offset: int = 0,
        search: Optional[str] = None,
    ) -> List[DataSourceInfo]:
        """
        List available datasets.

        Parameters
        ----------
        prefix : str
            Path prefix to filter (e.g. container name, directory).
        limit, offset : int
            Pagination.
        search : str, optional
            Substring search within names.
        """
        ...

    @abstractmethod
    async def read_dataset(
        self,
        path: str,
        format: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Read a dataset into a pandas DataFrame.

        Parameters
        ----------
        path : str
            Full qualified path / URI of the dataset.
        format : str, optional
            File format override (csv, parquet, json).  Auto-detected if None.
        limit : int, optional
            Maximum rows to read.
        """
        ...

    @abstractmethod
    async def dataset_exists(self, path: str) -> bool:
        """Check if the dataset at *path* exists."""
        ...

    @abstractmethod
    def provider_name(self) -> str:
        """Human-readable provider name (e.g. 'local', 'azure_blob')."""
        ...

    async def list_containers(self) -> List[Dict[str, Any]]:
        """
        List top-level containers / filesystems.

        Default: not supported (raises NotImplementedError).
        Override in cloud providers.
        """
        raise NotImplementedError(
            f"{self.provider_name()} does not support container listing."
        )