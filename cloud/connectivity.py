# cloud/connectivity.py
"""
Gateway registry and endpoint resolver for LIZARD cloud mode.

Responsibilities:
  • Maintain an in-memory registry of configured gateways.
  • Resolve Databricks workspace URLs (direct or via gateway).
  • Resolve Azure Storage endpoint URLs (Blob / DFS, direct or via gateway).
  • Validate that a resource is actually exposed through a given gateway.
"""
from __future__ import annotations

from typing import List, Optional

from cloud.config import GatewayConfig, LizardCloudConfig
from cloud.constants import (
    BLOB_DIRECT_ENDPOINT_TEMPLATE,
    BLOB_GATEWAY_ENDPOINT_TEMPLATE,
    CONNECTIVITY_DIRECT,
    CONNECTIVITY_GATEWAY,
    DATABRICKS_DIRECT_HOST_TEMPLATE,
    DATABRICKS_GATEWAY_HOST_TEMPLATE,
    DFS_DIRECT_ENDPOINT_TEMPLATE,
    DFS_GATEWAY_ENDPOINT_TEMPLATE,
    ENDPOINT_BLOB,
    ENDPOINT_DFS,
)
from cloud.diagnostics import ConfigurationError, GatewayExposureError


# ============================================================
# Gateway Registry
# ============================================================


class GatewayRegistry:
    """
    In-memory lookup for configured Application Gateways.

    Built from a ``LizardCloudConfig`` and refreshed when config changes.
    """

    def __init__(self, gateways: Optional[List[GatewayConfig]] = None) -> None:
        self._gateways: List[GatewayConfig] = list(gateways or [])

    # ---- lookups -----------------------------------------------------------

    def get_by_name(self, name: str) -> Optional[GatewayConfig]:
        lower = name.lower()
        for gw in self._gateways:
            if gw.name.lower() == lower:
                return gw
        return None

    def get_by_environment(self, env: str) -> List[GatewayConfig]:
        lower = env.lower()
        return [gw for gw in self._gateways if gw.environment.lower() == lower]

    def gateways_for_workspace(self, workspace_id: str) -> List[GatewayConfig]:
        """Return all gateways that expose the given Databricks workspace."""
        return [
            gw
            for gw in self._gateways
            if workspace_id in gw.exposed_workspaces
        ]

    def gateways_for_storage(self, account_name: str) -> List[GatewayConfig]:
        """Return all gateways that expose the given storage account."""
        return [
            gw
            for gw in self._gateways
            if account_name in gw.exposed_storage_accounts
        ]

    @property
    def all_gateways(self) -> List[GatewayConfig]:
        return list(self._gateways)

    def reload(self, gateways: List[GatewayConfig]) -> None:
        self._gateways = list(gateways)

    @classmethod
    def from_config(cls, config: LizardCloudConfig) -> "GatewayRegistry":
        return cls(gateways=config.gateways)


# ============================================================
# Endpoint Resolver
# ============================================================


class EndpointResolver:
    """
    Stateless helper that resolves endpoint URLs for Databricks workspaces
    and Azure Storage accounts, supporting both direct and gateway routing.
    """

    def __init__(self, registry: GatewayRegistry) -> None:
        self._registry = registry

    # ---- Databricks -------------------------------------------------------

    def resolve_databricks_host(
        self,
        workspace_id: str,
        connectivity: str = CONNECTIVITY_DIRECT,
        gateway_name: Optional[str] = None,
    ) -> str:
        """
        Return the base URL for a Databricks workspace.

        Parameters
        ----------
        workspace_id : str
            Numeric Databricks workspace ID.
        connectivity : str
            ``"direct"`` or ``"gateway"``.
        gateway_name : str, optional
            Required when *connectivity* is ``"gateway"``.

        Raises
        ------
        ConfigurationError
            If gateway routing is requested but *gateway_name* is missing.
        GatewayExposureError
            If the workspace is not exposed through the chosen gateway.
        """
        if connectivity == CONNECTIVITY_DIRECT:
            return DATABRICKS_DIRECT_HOST_TEMPLATE.format(
                workspace_id=workspace_id
            )

        # --- gateway ---
        if not gateway_name:
            raise ConfigurationError(
                message="Gateway connectivity selected but no gateway_name provided.",
                action="Set 'gateway_name' on the Databricks connection, or switch to 'direct' connectivity.",
                context={"workspace_id": workspace_id},
            )

        gateway = self._registry.get_by_name(gateway_name)
        if gateway is None:
            available = [gw.name for gw in self._registry.all_gateways]
            raise ConfigurationError(
                message=f"Gateway '{gateway_name}' not found in configuration.",
                action=f"Check your gateway definitions. Available gateways: {available}.",
                context={
                    "gateway_name": gateway_name,
                    "available_gateways": available,
                },
            )

        self._validate_workspace_exposure(workspace_id, gateway)

        return DATABRICKS_GATEWAY_HOST_TEMPLATE.format(
            gateway_fqdn=gateway.fqdn,
            workspace_id=workspace_id,
        )

    # ---- Azure Storage ----------------------------------------------------

    def resolve_storage_url(
        self,
        account_name: str,
        connectivity: str = CONNECTIVITY_DIRECT,
        gateway_name: Optional[str] = None,
        endpoint_type: str = ENDPOINT_BLOB,
    ) -> str:
        """
        Return the base URL for an Azure Storage account.

        Parameters
        ----------
        account_name : str
            Azure storage account name.
        connectivity : str
            ``"direct"`` or ``"gateway"``.
        gateway_name : str, optional
            Required when *connectivity* is ``"gateway"``.
        endpoint_type : str
            ``"blob"`` or ``"dfs"`` (ADLS Gen2).

        Raises
        ------
        ConfigurationError
            If gateway routing is requested but *gateway_name* is missing.
        GatewayExposureError
            If the storage account is not exposed through the chosen gateway.
        """
        if connectivity == CONNECTIVITY_DIRECT:
            if endpoint_type == ENDPOINT_DFS:
                return DFS_DIRECT_ENDPOINT_TEMPLATE.format(
                    account_name=account_name
                )
            return BLOB_DIRECT_ENDPOINT_TEMPLATE.format(
                account_name=account_name
            )

        # --- gateway ---
        if not gateway_name:
            raise ConfigurationError(
                message="Gateway connectivity selected but no gateway_name provided.",
                action="Set 'gateway_name' on the storage connection, or switch to 'direct' connectivity.",
                context={"account_name": account_name},
            )

        gateway = self._registry.get_by_name(gateway_name)
        if gateway is None:
            available = [gw.name for gw in self._registry.all_gateways]
            raise ConfigurationError(
                message=f"Gateway '{gateway_name}' not found in configuration.",
                action=f"Check your gateway definitions. Available gateways: {available}.",
                context={
                    "gateway_name": gateway_name,
                    "available_gateways": available,
                },
            )

        self._validate_storage_exposure(account_name, gateway)

        if endpoint_type == ENDPOINT_DFS:
            return DFS_GATEWAY_ENDPOINT_TEMPLATE.format(
                gateway_fqdn=gateway.fqdn,
                account_name=account_name,
            )
        return BLOB_GATEWAY_ENDPOINT_TEMPLATE.format(
            gateway_fqdn=gateway.fqdn,
            account_name=account_name,
        )

    # ---- exposure validation ----------------------------------------------

    def _validate_workspace_exposure(
        self, workspace_id: str, gateway: GatewayConfig
    ) -> None:
        if workspace_id not in gateway.exposed_workspaces:
            alt_gateways = self._registry.gateways_for_workspace(workspace_id)
            alt_names = [gw.name for gw in alt_gateways]
            raise GatewayExposureError(
                message=(
                    f"Databricks workspace '{workspace_id}' is not exposed "
                    f"through gateway '{gateway.name}' ({gateway.fqdn})."
                ),
                action=self._build_exposure_action(
                    resource_kind="workspace",
                    resource_id=workspace_id,
                    gateway_name=gateway.name,
                    alternatives=alt_names,
                ),
                context={
                    "workspace_id": workspace_id,
                    "gateway": gateway.name,
                    "exposed_workspaces": gateway.exposed_workspaces,
                    "alternative_gateways": alt_names,
                },
            )

    def _validate_storage_exposure(
        self, account_name: str, gateway: GatewayConfig
    ) -> None:
        if account_name not in gateway.exposed_storage_accounts:
            alt_gateways = self._registry.gateways_for_storage(account_name)
            alt_names = [gw.name for gw in alt_gateways]
            raise GatewayExposureError(
                message=(
                    f"Storage account '{account_name}' is not exposed "
                    f"through gateway '{gateway.name}' ({gateway.fqdn})."
                ),
                action=self._build_exposure_action(
                    resource_kind="storage account",
                    resource_id=account_name,
                    gateway_name=gateway.name,
                    alternatives=alt_names,
                ),
                context={
                    "account_name": account_name,
                    "gateway": gateway.name,
                    "exposed_storage_accounts": gateway.exposed_storage_accounts,
                    "alternative_gateways": alt_names,
                },
            )

    @staticmethod
    def _build_exposure_action(
        resource_kind: str,
        resource_id: str,
        gateway_name: str,
        alternatives: List[str],
    ) -> str:
        parts: list[str] = []
        if alternatives:
            parts.append(
                f"The {resource_kind} '{resource_id}' IS exposed through: "
                f"{alternatives}. Switch your connection to one of those gateways."
            )
        else:
            parts.append(
                f"No configured gateway exposes this {resource_kind}. "
                f"Either add '{resource_id}' to gateway '{gateway_name}' "
                f"exposed list, or switch to 'direct' connectivity."
            )
        return " ".join(parts)