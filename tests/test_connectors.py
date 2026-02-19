# tests/test_connectors.py
"""Tests for the DataSourceProvider abstraction and LocalDataSourceProvider."""
from __future__ import annotations

import pytest
import pandas as pd

from connectors.base import DataSourceInfo, DataSourceProvider
from connectors.local_provider import LocalDataSourceProvider


# ── DataSourceInfo ──────────────────────────────��─────────────────────

def test_datasource_info_creation():
    info = DataSourceInfo(
        name="fraud.csv",
        path="/data/fraud.csv",
        provider="local",
        size_bytes=1024,
        last_modified="2025-01-01T00:00:00Z",
        content_type="text/csv",
    )
    assert info.name == "fraud.csv"
    assert info.provider == "local"
    assert info.size_bytes == 1024


def test_datasource_info_to_dict_excludes_none():
    info = DataSourceInfo(
        name="fraud.csv",
        path="/data/fraud.csv",
        provider="local",
    )
    d = info.to_dict()
    assert d["name"] == "fraud.csv"
    assert d["provider"] == "local"
    assert "container" not in d
    assert "account" not in d
    assert "size_bytes" not in d


def test_datasource_info_to_dict_includes_set_fields():
    info = DataSourceInfo(
        name="test.parquet",
        path="abfss://container@acc.dfs.core.windows.net/test.parquet",
        provider="azure_dfs",
        container="container",
        account="acc",
        size_bytes=2048,
    )
    d = info.to_dict()
    assert d["container"] == "container"
    assert d["account"] == "acc"
    assert d["size_bytes"] == 2048


# ── DataSourceProvider abstract interface ────────────────────────────

def test_provider_is_abstract():
    """DataSourceProvider cannot be instantiated directly."""
    with pytest.raises(TypeError):
        DataSourceProvider()


# ── LocalDataSourceProvider: provider_name ───────────────────────────

def test_local_provider_name():
    provider = LocalDataSourceProvider()
    assert provider.provider_name() == "local"


# ── LocalDataSourceProvider: list_sources ────────────────────────────

@pytest.mark.asyncio
async def test_local_provider_list_sources():
    async def mock_list():
        return [
            {"name": "transactions", "row_count": 100, "type": "table",
             "min_ts": "2025-01-01", "max_ts": "2025-01-31"},
            {"name": "users", "row_count": 50, "type": "table",
             "min_ts": "2025-01-01", "max_ts": "2025-01-15"},
        ]

    provider = LocalDataSourceProvider(list_sources_func=mock_list)
    sources = await provider.list_sources()
    assert len(sources) == 2
    assert sources[0].name == "transactions"
    assert sources[0].provider == "local"
    assert sources[1].name == "users"


@pytest.mark.asyncio
async def test_local_provider_list_sources_with_search():
    async def mock_list():
        return [
            {"name": "transactions", "row_count": 100, "type": "table"},
            {"name": "users", "row_count": 50, "type": "table"},
            {"name": "transaction_logs", "row_count": 200, "type": "table"},
        ]

    provider = LocalDataSourceProvider(list_sources_func=mock_list)
    sources = await provider.list_sources(search="trans")
    assert len(sources) == 2
    assert all("trans" in s.name.lower() for s in sources)


@pytest.mark.asyncio
async def test_local_provider_list_sources_with_pagination():
    async def mock_list():
        return [{"name": f"table_{i}", "row_count": i, "type": "table"} for i in range(10)]

    provider = LocalDataSourceProvider(list_sources_func=mock_list)
    page1 = await provider.list_sources(limit=3, offset=0)
    page2 = await provider.list_sources(limit=3, offset=3)
    page3 = await provider.list_sources(limit=3, offset=9)
    assert len(page1) == 3
    assert len(page2) == 3
    assert len(page3) == 1
    assert page1[0].name == "table_0"
    assert page2[0].name == "table_3"
    assert page3[0].name == "table_9"


@pytest.mark.asyncio
async def test_local_provider_list_sources_empty_when_no_func():
    provider = LocalDataSourceProvider()
    sources = await provider.list_sources()
    assert sources == []


# ── LocalDataSourceProvider: read_dataset ────────────────────────────

@pytest.mark.asyncio
async def test_local_provider_read_dataset():
    df_expected = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    async def mock_read(name: str):
        return df_expected.copy()

    provider = LocalDataSourceProvider(get_source_data_func=mock_read)
    df = await provider.read_dataset("test_table")
    assert len(df) == 3
    assert list(df.columns) == ["a", "b"]


@pytest.mark.asyncio
async def test_local_provider_read_dataset_with_limit():
    df_big = pd.DataFrame({"val": range(100)})

    async def mock_read(name: str):
        return df_big.copy()

    provider = LocalDataSourceProvider(get_source_data_func=mock_read)
    df = await provider.read_dataset("big_table", limit=5)
    assert len(df) == 5


@pytest.mark.asyncio
async def test_local_provider_read_dataset_empty_when_no_func():
    provider = LocalDataSourceProvider()
    df = await provider.read_dataset("anything")
    assert df.empty


# ── LocalDataSourceProvider: dataset_exists ──────────────────────────

@pytest.mark.asyncio
async def test_local_provider_dataset_exists_true():
    df = pd.DataFrame({"x": [1, 2, 3]})

    async def mock_read(name: str):
        if name == "exists":
            return df.copy()
        return pd.DataFrame()

    provider = LocalDataSourceProvider(get_source_data_func=mock_read)
    assert await provider.dataset_exists("exists") is True


@pytest.mark.asyncio
async def test_local_provider_dataset_exists_false():
    async def mock_read(name: str):
        return pd.DataFrame()

    provider = LocalDataSourceProvider(get_source_data_func=mock_read)
    assert await provider.dataset_exists("nope") is False


# ── LocalDataSourceProvider: list_containers not supported ───────────

@pytest.mark.asyncio
async def test_local_provider_list_containers_not_supported():
    provider = LocalDataSourceProvider()
    with pytest.raises(NotImplementedError):
        await provider.list_containers()