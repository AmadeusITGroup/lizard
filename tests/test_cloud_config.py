# tests/test_cloud_config.py
"""Unit tests for cloud/config.py — config loading, YAML round-trip, env overrides."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from cloud.config import (
    LizardCloudConfig,
    GatewayConfig,
    DatabricksConnectionConfig,
    StorageConnectionConfig,
    AuthConfig,
    ClusterConfig,
    load_config_from_yaml,
    save_config_to_yaml,
    load_config_from_env,
    get_config,
    set_config,
    reset_config,
    _resolve_env_vars,
)


# ---- defaults ----

def test_default_config_is_local():
    cfg = LizardCloudConfig()
    assert cfg.mode == "local"
    assert cfg.is_cloud_mode is False
    assert cfg.gateways == []
    assert cfg.databricks_connections == []
    assert cfg.storage_connections == []


# ---- YAML round-trip ----

def test_yaml_round_trip():
    original = LizardCloudConfig(
        mode="cloud",
        gateways=[
            GatewayConfig(
                name="tst-gw",
                fqdn="gw.corp.com",
                environment="TST",
                exposed_workspaces=["111", "222"],
                exposed_storage_accounts=["stgtest"],
            )
        ],
        databricks_connections=[
            DatabricksConnectionConfig(
                name="tst-ws",
                workspace_id="111",
                connectivity="gateway",
                gateway_name="tst-gw",
                auth=AuthConfig(type="developer_token", token="dapi_test"),
                cluster=ClusterConfig(cluster_id="0101-abc"),
            )
        ],
        storage_connections=[
            StorageConnectionConfig(
                name="tst-blob",
                account_name="stgtest",
                container="data",
                endpoint_type="blob",
                connectivity="direct",
                auth=AuthConfig(type="service_principal", tenant_id="t", client_id="c", client_secret="s"),
            )
        ],
    )

    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
        path = f.name

    try:
        save_config_to_yaml(original, path)
        loaded = load_config_from_yaml(path)

        assert loaded.mode == "cloud"
        assert len(loaded.gateways) == 1
        assert loaded.gateways[0].name == "tst-gw"
        assert loaded.gateways[0].exposed_workspaces == ["111", "222"]
        assert len(loaded.databricks_connections) == 1
        assert loaded.databricks_connections[0].workspace_id == "111"
        assert loaded.databricks_connections[0].connectivity == "gateway"
        assert len(loaded.storage_connections) == 1
        assert loaded.storage_connections[0].account_name == "stgtest"
    finally:
        os.unlink(path)


def test_load_missing_yaml_returns_default():
    cfg = load_config_from_yaml("/nonexistent/lizard-cloud.yaml")
    assert cfg.mode == "local"


# ---- env-var overrides ----

def test_env_var_mode_override(monkeypatch):
    monkeypatch.setenv("LIZARD_MODE", "cloud")
    overrides = load_config_from_env()
    assert overrides["mode"] == "cloud"


def test_env_var_invalid_mode_ignored(monkeypatch):
    monkeypatch.setenv("LIZARD_MODE", "banana")
    overrides = load_config_from_env()
    assert "mode" not in overrides


def test_env_var_not_set():
    overrides = load_config_from_env()
    # may or may not have mode depending on env; at least should not error
    assert isinstance(overrides, dict)


# ---- env-var placeholder resolution ----

def test_resolve_env_vars(monkeypatch):
    monkeypatch.setenv("MY_TENANT", "tenant-abc")
    result = _resolve_env_vars({"tenant_id": "${MY_TENANT}", "other": "plain"})
    assert result == {"tenant_id": "tenant-abc", "other": "plain"}


def test_resolve_env_vars_missing_left_as_is():
    result = _resolve_env_vars("${UNLIKELY_VAR_XYZ}")
    assert result == "${UNLIKELY_VAR_XYZ}"


# ---- singleton ----

def test_singleton_get_set():
    reset_config()
    cfg = LizardCloudConfig(mode="cloud")
    set_config(cfg)
    assert get_config().mode == "cloud"
    reset_config()


# ---- lookup helpers ----

def test_gateway_lookup():
    cfg = LizardCloudConfig(
        gateways=[
            GatewayConfig(name="Alpha", fqdn="a.com"),
            GatewayConfig(name="Beta", fqdn="b.com"),
        ]
    )
    assert cfg.get_gateway("alpha") is not None
    assert cfg.get_gateway("alpha").fqdn == "a.com"
    assert cfg.get_gateway("BETA").fqdn == "b.com"
    assert cfg.get_gateway("gamma") is None


def test_connection_lookups():
    cfg = LizardCloudConfig(
        databricks_connections=[
            DatabricksConnectionConfig(name="ws1", workspace_id="1")
        ],
        storage_connections=[
            StorageConnectionConfig(name="stg1", account_name="acc1")
        ],
    )
    assert cfg.get_databricks_connection("WS1").workspace_id == "1"
    assert cfg.get_databricks_connection("nope") is None
    assert cfg.get_storage_connection("STG1").account_name == "acc1"
    assert cfg.get_storage_connection("nope") is None