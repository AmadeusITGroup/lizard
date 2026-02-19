# tests/test_output_storage.py
"""
Tests for cloud/output_storage.py.

Covers:
  - _generate_filename: timestamped filename generation
  - _df_to_bytes: CSV and Parquet serialization
  - export_to_blob_storage: full export flow (mocked provider/HTTP)
  - export_to_dbfs: full export flow (mocked HTTP)
  - Error cases: missing connection, missing container, unsupported format
"""
from __future__ import annotations

import io
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from cloud.diagnostics import ConfigurationError, ConnectivityError


# ── _generate_filename ───────────────────────────────────────────────


def test_generate_filename_csv():
    from cloud.output_storage import _generate_filename

    name = _generate_filename("export", "csv")
    assert name.startswith("export_")
    assert name.endswith(".csv")
    # Timestamp format: YYYYMMDD_HHMMSS
    parts = name.replace("export_", "").replace(".csv", "")
    assert len(parts) == 15  # 8 date + 1 underscore + 6 time


def test_generate_filename_parquet():
    from cloud.output_storage import _generate_filename

    name = _generate_filename("results", "parquet")
    assert name.startswith("results_")
    assert name.endswith(".parquet")


def test_generate_filename_custom_prefix():
    from cloud.output_storage import _generate_filename

    name = _generate_filename("my-analysis", "csv")
    assert name.startswith("my-analysis_")


# ── _df_to_bytes ─────────────────────────────────────────────────────


def test_df_to_bytes_csv():
    from cloud.output_storage import _df_to_bytes

    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    data = _df_to_bytes(df, "csv")

    assert isinstance(data, bytes)
    text = data.decode("utf-8")
    assert "a,b" in text
    assert "1,x" in text
    assert "3,z" in text


def test_df_to_bytes_parquet():
    from cloud.output_storage import _df_to_bytes

    df = pd.DataFrame({"col1": [10, 20], "col2": ["hello", "world"]})
    data = _df_to_bytes(df, "parquet")

    assert isinstance(data, bytes)
    assert len(data) > 0
    # Verify it's valid parquet by reading it back
    result = pd.read_parquet(io.BytesIO(data))
    assert list(result.columns) == ["col1", "col2"]
    assert len(result) == 2
    assert result["col1"].tolist() == [10, 20]


def test_df_to_bytes_unsupported_format():
    from cloud.output_storage import _df_to_bytes

    df = pd.DataFrame({"a": [1]})
    with pytest.raises(ValueError, match="Unsupported format"):
        _df_to_bytes(df, "json")


def test_df_to_bytes_empty_df():
    from cloud.output_storage import _df_to_bytes

    df = pd.DataFrame({"a": pd.Series(dtype=int), "b": pd.Series(dtype=str)})
    data = _df_to_bytes(df, "csv")
    text = data.decode("utf-8")
    assert "a,b" in text  # Header still present


def test_df_to_bytes_csv_roundtrip():
    from cloud.output_storage import _df_to_bytes

    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [95.5, 87.3, 92.1],
    })
    data = _df_to_bytes(df, "csv")
    result = pd.read_csv(io.BytesIO(data))
    assert list(result.columns) == ["id", "name", "score"]
    assert len(result) == 3


def test_df_to_bytes_parquet_roundtrip():
    from cloud.output_storage import _df_to_bytes

    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [95.5, 87.3, 92.1],
    })
    data = _df_to_bytes(df, "parquet")
    result = pd.read_parquet(io.BytesIO(data))
    assert list(result.columns) == ["id", "name", "score"]
    assert result["score"].tolist() == [95.5, 87.3, 92.1]


# ── export_to_blob_storage ───────────────────────────────────────────


def _mock_cloud_config_with_storage(conn_name="my_storage", container="exports"):
    """Build a mock LizardCloudConfig with one storage connection."""
    from cloud.config import (
        LizardCloudConfig,
        StorageConnectionConfig,
        AuthConfig,
    )
    conn = StorageConnectionConfig(
        name=conn_name,
        account_name="teststorage",
        container=container,
        endpoint_type="blob",
        connectivity="direct",
        auth=AuthConfig(type="service_principal", client_id="cid", client_secret="sec", tenant_id="tid"),
    )
    return LizardCloudConfig(mode="cloud", storage_connections=[conn])


@pytest.mark.asyncio
async def test_export_to_blob_storage_success():
    from cloud.output_storage import export_to_blob_storage

    df = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    cfg = _mock_cloud_config_with_storage()

    mock_provider = MagicMock()
    mock_provider.write_bytes = AsyncMock()

    with patch("cloud.output_storage.get_config", return_value=cfg), \
         patch("connectors.provider_factory.create_storage_provider", return_value=mock_provider):
        result = await export_to_blob_storage(
            df=df,
            connection_name="my_storage",
            format="csv",
        )

    assert result["status"] == "ok"
    assert result["connection"] == "my_storage"
    assert result["container"] == "exports"
    assert result["format"] == "csv"
    assert result["rows"] == 3
    assert "x" in result["columns"]
    assert "y" in result["columns"]
    assert result["size_bytes"] > 0
    assert result["filename"].endswith(".csv")

    mock_provider.write_bytes.assert_called_once()


@pytest.mark.asyncio
async def test_export_to_blob_storage_parquet():
    from cloud.output_storage import export_to_blob_storage

    df = pd.DataFrame({"id": [10, 20]})
    cfg = _mock_cloud_config_with_storage()

    mock_provider = MagicMock()
    mock_provider.write_bytes = AsyncMock()

    with patch("cloud.output_storage.get_config", return_value=cfg), \
         patch("connectors.provider_factory.create_storage_provider", return_value=mock_provider):
        result = await export_to_blob_storage(
            df=df,
            connection_name="my_storage",
            format="parquet",
        )

    assert result["format"] == "parquet"
    assert result["filename"].endswith(".parquet")
    assert result["rows"] == 2


@pytest.mark.asyncio
async def test_export_to_blob_storage_custom_filename():
    from cloud.output_storage import export_to_blob_storage

    df = pd.DataFrame({"a": [1]})
    cfg = _mock_cloud_config_with_storage()

    mock_provider = MagicMock()
    mock_provider.write_bytes = AsyncMock()

    with patch("cloud.output_storage.get_config", return_value=cfg), \
         patch("connectors.provider_factory.create_storage_provider", return_value=mock_provider):
        result = await export_to_blob_storage(
            df=df,
            connection_name="my_storage",
            filename="my_custom_file.csv",
            format="csv",
        )

    assert result["filename"] == "my_custom_file.csv"
    assert result["path"] == "lizard-exports/my_custom_file.csv"


@pytest.mark.asyncio
async def test_export_to_blob_storage_missing_connection():
    from cloud.output_storage import export_to_blob_storage
    from cloud.config import LizardCloudConfig

    df = pd.DataFrame({"a": [1]})

    with patch("cloud.output_storage.get_config", return_value=LizardCloudConfig()):
        with pytest.raises(ConfigurationError, match="not found"):
            await export_to_blob_storage(df=df, connection_name="nonexistent")


@pytest.mark.asyncio
async def test_export_to_blob_storage_missing_container():
    from cloud.output_storage import export_to_blob_storage

    df = pd.DataFrame({"a": [1]})
    cfg = _mock_cloud_config_with_storage(container="")  # Empty container

    with patch("cloud.output_storage.get_config", return_value=cfg):
        with pytest.raises(ConfigurationError, match="No container"):
            await export_to_blob_storage(
                df=df,
                connection_name="my_storage",
                container="",  # Also no override
            )


@pytest.mark.asyncio
async def test_export_to_blob_storage_fallback_to_raw():
    """When provider doesn't have write_bytes, falls back to _upload_blob_raw."""
    from cloud.output_storage import export_to_blob_storage

    df = pd.DataFrame({"a": [1, 2]})
    cfg = _mock_cloud_config_with_storage()

    # Provider without write_bytes
    mock_provider = MagicMock(spec=[])  # Empty spec = no write_bytes

    with patch("cloud.output_storage.get_config", return_value=cfg), \
         patch("connectors.provider_factory.create_storage_provider", return_value=mock_provider), \
         patch("cloud.output_storage._upload_blob_raw", new_callable=AsyncMock) as mock_raw:
        result = await export_to_blob_storage(
            df=df,
            connection_name="my_storage",
            format="csv",
        )

    assert result["status"] == "ok"
    mock_raw.assert_called_once()


# ── export_to_dbfs ���──────────────────────────────────────────────────


def _mock_cloud_config_with_databricks(conn_name="my_workspace"):
    from cloud.config import (
        LizardCloudConfig,
        DatabricksConnectionConfig,
        AuthConfig,
        ClusterConfig,
    )
    conn = DatabricksConnectionConfig(
        name=conn_name,
        workspace_id="ws-123",
        connectivity="direct",
        auth=AuthConfig(type="service_principal", client_id="cid", client_secret="sec", tenant_id="tid"),
        cluster=ClusterConfig(cluster_id="c1"),
    )
    return LizardCloudConfig(mode="cloud", databricks_connections=[conn])


@pytest.mark.asyncio
async def test_export_to_dbfs_success():
    from cloud.output_storage import export_to_dbfs

    df = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    cfg = _mock_cloud_config_with_databricks()

    with patch("cloud.output_storage.get_config", return_value=cfg):
        with patch("cloud.output_storage._upload_dbfs_raw", new_callable=AsyncMock) as mock_upload:
            result = await export_to_dbfs(
                df=df,
                connection_name="my_workspace",
                format="parquet",
            )

    assert result["status"] == "ok"
    assert result["connection"] == "my_workspace"
    assert result["format"] == "parquet"
    assert result["rows"] == 2
    assert result["path"].startswith("/lizard-exports/")
    mock_upload.assert_called_once()


@pytest.mark.asyncio
async def test_export_to_dbfs_csv():
    from cloud.output_storage import export_to_dbfs

    df = pd.DataFrame({"val": [42]})
    cfg = _mock_cloud_config_with_databricks()

    with patch("cloud.output_storage.get_config", return_value=cfg):
        with patch("cloud.output_storage._upload_dbfs_raw", new_callable=AsyncMock):
            result = await export_to_dbfs(
                df=df,
                connection_name="my_workspace",
                format="csv",
                path_prefix="/my-output",
            )

    assert result["format"] == "csv"
    assert result["path"].startswith("/my-output/")


@pytest.mark.asyncio
async def test_export_to_dbfs_missing_connection():
    from cloud.output_storage import export_to_dbfs
    from cloud.config import LizardCloudConfig

    df = pd.DataFrame({"a": [1]})

    with patch("cloud.output_storage.get_config", return_value=LizardCloudConfig()):
        with pytest.raises(ConfigurationError, match="not found"):
            await export_to_dbfs(df=df, connection_name="nonexistent")


@pytest.mark.asyncio
async def test_export_to_dbfs_upload_failure():
    from cloud.output_storage import export_to_dbfs

    df = pd.DataFrame({"a": [1]})
    cfg = _mock_cloud_config_with_databricks()

    with patch("cloud.output_storage.get_config", return_value=cfg):
        with patch("cloud.output_storage._upload_dbfs_raw", new_callable=AsyncMock, side_effect=Exception("Timeout")):
            with pytest.raises(ConnectivityError, match="Failed to write to DBFS"):
                await export_to_dbfs(df=df, connection_name="my_workspace")