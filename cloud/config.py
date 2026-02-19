# cloud/config.py
"""
LIZARD Cloud configuration system.

Configuration can be loaded from:
  1. A YAML file (lizard-cloud.yaml by default)
  2. Environment variables (overrides)
  3. The REST API (runtime updates, persisted back to YAML)

When no configuration exists the default mode is "local" and all cloud
features are inert.
"""
from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from cloud.constants import (
    CONNECTIVITY_DIRECT,
    CONNECTIVITY_GATEWAY,
    DEFAULT_CONFIG_FILENAME,
)

# ============================================================
# Sub-models
# ============================================================


class AuthConfig(BaseModel):
    """Authentication configuration for a single connection."""

    type: Literal["service_principal", "developer_token", "username_password"] = (
        "service_principal"
    )
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    # Developer token (PAT) — for local testing only
    token: Optional[str] = None
    # Username/password — where allowed
    username: Optional[str] = None
    password: Optional[str] = None


class ClusterConfig(BaseModel):
    """Databricks cluster / warehouse selection & Spark settings."""

    cluster_id: Optional[str] = None
    cluster_name: Optional[str] = None
    warehouse_id: Optional[str] = None       # SQL Warehouse for Spark SQL execution
    warehouse_name: Optional[str] = None
    spark_config: Dict[str, str] = Field(default_factory=dict)
    auto_start: bool = True


class GatewayConfig(BaseModel):
    """Definition of a single Application Gateway."""

    name: str
    fqdn: str
    environment: str = ""  # e.g. "TST", "CCP", "PRD"
    exposed_workspaces: List[str] = Field(default_factory=list)
    exposed_storage_accounts: List[str] = Field(default_factory=list)


class DatabricksConnectionConfig(BaseModel):
    """Configuration for connecting to a Databricks workspace."""

    name: str
    workspace_id: str
    workspace_url: Optional[str] = None  # override, usually computed
    connectivity: Literal["direct", "gateway"] = CONNECTIVITY_DIRECT
    gateway_name: Optional[str] = None  # required when connectivity == "gateway"
    auth: AuthConfig = Field(default_factory=AuthConfig)
    cluster: ClusterConfig = Field(default_factory=ClusterConfig)

    # ---- convenience accessors for Phase 3 engine layer ----

    @property
    def cluster_id(self) -> Optional[str]:
        """Shortcut: the interactive cluster ID from the nested ClusterConfig."""
        return self.cluster.cluster_id

    @cluster_id.setter
    def cluster_id(self, value: Optional[str]) -> None:
        self.cluster.cluster_id = value

    @property
    def warehouse_id(self) -> Optional[str]:
        """Shortcut: the SQL Warehouse ID from the nested ClusterConfig."""
        return self.cluster.warehouse_id

    @warehouse_id.setter
    def warehouse_id(self, value: Optional[str]) -> None:
        self.cluster.warehouse_id = value


class StorageConnectionConfig(BaseModel):
    """Configuration for connecting to an Azure Storage account."""

    name: str
    account_name: str
    container: str = ""
    endpoint_type: Literal["blob", "dfs"] = "blob"
    connectivity: Literal["direct", "gateway"] = CONNECTIVITY_DIRECT
    gateway_name: Optional[str] = None
    auth: AuthConfig = Field(default_factory=AuthConfig)


# ============================================================
# Top-level configuration
# ============================================================


class LizardCloudConfig(BaseModel):
    """
    Root configuration for LIZARD cloud mode.

    Defaults to ``mode="local"`` so that existing users are unaffected.
    """

    mode: Literal["local", "cloud"] = "local"
    gateways: List[GatewayConfig] = Field(default_factory=list)
    databricks_connections: List[DatabricksConnectionConfig] = Field(
        default_factory=list
    )
    storage_connections: List[StorageConnectionConfig] = Field(default_factory=list)

    # ---- helpers ----------------------------------------------------------

    def get_gateway(self, name: str) -> Optional[GatewayConfig]:
        """Look up a gateway by name (case-insensitive)."""
        lower = name.lower()
        for gw in self.gateways:
            if gw.name.lower() == lower:
                return gw
        return None

    def get_databricks_connection(
        self, name: str
    ) -> Optional[DatabricksConnectionConfig]:
        lower = name.lower()
        for conn in self.databricks_connections:
            if conn.name.lower() == lower:
                return conn
        return None

    def get_storage_connection(
        self, name: str
    ) -> Optional[StorageConnectionConfig]:
        lower = name.lower()
        for conn in self.storage_connections:
            if conn.name.lower() == lower:
                return conn
        return None

    @property
    def is_cloud_mode(self) -> bool:
        return self.mode == "cloud"


# ============================================================
# YAML load / save helpers
# ============================================================


def _resolve_env_vars(raw: Any) -> Any:
    """
    Recursively replace ``${ENV_VAR}`` placeholders with their values.

    If the env-var is not set the placeholder is left as-is so the user
    sees exactly what is missing.
    """
    if isinstance(raw, str):
        import re

        def _replace(m: re.Match) -> str:
            var = m.group(1)
            return os.environ.get(var, m.group(0))

        return re.sub(r"\$\{(\w+)}", _replace, raw)
    if isinstance(raw, dict):
        return {k: _resolve_env_vars(v) for k, v in raw.items()}
    if isinstance(raw, list):
        return [_resolve_env_vars(v) for v in raw]
    return raw


def load_config_from_yaml(path: str | Path) -> LizardCloudConfig:
    """Load a ``LizardCloudConfig`` from a YAML file."""
    import yaml  # pyyaml is already a project dependency

    p = Path(path)
    if not p.exists():
        return LizardCloudConfig()

    with open(p, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    resolved = _resolve_env_vars(raw)
    return LizardCloudConfig(**resolved)


def save_config_to_yaml(config: LizardCloudConfig, path: str | Path) -> None:
    """Persist a ``LizardCloudConfig`` to a YAML file."""
    import yaml

    p = Path(path)
    data = config.model_dump(mode="json", exclude_none=True)
    with open(p, "w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, default_flow_style=False, sort_keys=False)


def save_config(config: LizardCloudConfig) -> None:
    """Convenience: persist the given config to the default YAML path and update singleton."""
    yaml_path = os.environ.get("LIZARD_CLOUD_CONFIG_PATH", DEFAULT_CONFIG_FILENAME)
    save_config_to_yaml(config, yaml_path)
    set_config(config)


def load_config_from_env() -> Dict[str, Any]:
    """
    Build partial config overrides from environment variables.

    Supported env vars (all optional):
      LIZARD_MODE              → mode
      LIZARD_CLOUD_CONFIG_PATH → (used by get_config)
      AZURE_TENANT_ID          → injected into auth blocks
      AZURE_CLIENT_ID          → injected into auth blocks
      AZURE_CLIENT_SECRET      → injected into auth blocks
    """
    overrides: Dict[str, Any] = {}
    mode = os.environ.get("LIZARD_MODE")
    if mode and mode in ("local", "cloud"):
        overrides["mode"] = mode
    return overrides


# ============================================================
# Singleton accessor (thread-safe, lazy)
# ============================================================

_config_lock = threading.Lock()
_config_instance: Optional[LizardCloudConfig] = None


def get_config(*, reload: bool = False) -> LizardCloudConfig:
    """
    Return the global ``LizardCloudConfig`` singleton.

    On first call (or when *reload* is True) the config is loaded from the
    YAML file pointed to by ``LIZARD_CLOUD_CONFIG_PATH`` env-var (defaulting
    to ``lizard-cloud.yaml`` in the project root), then any env-var overrides
    are applied on top.
    """
    global _config_instance  # noqa: PLW0603

    if _config_instance is not None and not reload:
        return _config_instance

    with _config_lock:
        # Double-checked locking
        if _config_instance is not None and not reload:
            return _config_instance

        yaml_path = os.environ.get(
            "LIZARD_CLOUD_CONFIG_PATH", DEFAULT_CONFIG_FILENAME
        )
        cfg = load_config_from_yaml(yaml_path)

        # Apply env-var overrides
        env_overrides = load_config_from_env()
        if env_overrides:
            data = cfg.model_dump()
            data.update(env_overrides)
            cfg = LizardCloudConfig(**data)

        _config_instance = cfg
        return _config_instance


def set_config(config: LizardCloudConfig) -> None:
    """Replace the global singleton (e.g. from the REST API)."""
    global _config_instance  # noqa: PLW0603
    with _config_lock:
        _config_instance = config


def reset_config() -> None:
    """Reset the singleton so the next ``get_config()`` reloads from disk."""
    global _config_instance  # noqa: PLW0603
    with _config_lock:
        _config_instance = None