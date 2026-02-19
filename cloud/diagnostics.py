# cloud/diagnostics.py
"""
LIZARD Cloud error hierarchy.

Every error carries:
  • message   – what went wrong
  • action    – what the user should do to fix it
  • context   – structured dict for logging / UI display
"""
from __future__ import annotations

from typing import Any, Dict


class LizardCloudError(Exception):
    """Base class for all LIZARD cloud-mode errors."""

    def __init__(
        self,
        message: str,
        action: str = "",
        context: Dict[str, Any] | None = None,
    ) -> None:
        self.message = message
        self.action = action
        self.context = context or {}
        super().__init__(self._full_message())

    def _full_message(self) -> str:
        parts = [self.message]
        if self.action:
            parts.append(f"Action: {self.action}")
        return " | ".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise for JSON API responses."""
        return {
            "error_type": type(self).__name__,
            "message": self.message,
            "action": self.action,
            "context": self.context,
        }


class GatewayExposureError(LizardCloudError):
    """
    A resource (Databricks workspace or storage account) is not exposed
    through the selected Application Gateway.
    """

    pass


class ConnectivityError(LizardCloudError):
    """
    The target endpoint (gateway or direct) is not reachable.

    Typical causes: network/VPN restrictions, gateway down, DNS failure.
    """

    pass


class AuthenticationError(LizardCloudError):
    """
    Authentication failed — bad credentials, expired token, missing RBAC.
    """

    pass


class ClusterNotAvailableError(LizardCloudError):
    """
    The Databricks cluster specified in the configuration is not running,
    does not exist, or cannot be started.
    """

    pass


class ConfigurationError(LizardCloudError):
    """
    The cloud configuration is invalid or incomplete.
    """

    pass