# cloud/auth.py
"""
Authentication providers for LIZARD cloud mode.

All Azure SDK imports are lazy-guarded so this module is safe to import
even when the ``azure-identity`` package is not installed.
"""
from __future__ import annotations

import time
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from cloud.config import AuthConfig
from cloud.diagnostics import AuthenticationError, ConfigurationError


# ============================================================
# Token dataclass
# ============================================================


@dataclass
class AccessToken:
    """Minimal representation of an OAuth access token."""

    token: str
    expires_on: float  # epoch seconds
    scope: str = ""

    @property
    def is_expired(self) -> bool:
        # Consider expired 60 s before actual expiry to give a buffer
        return time.time() >= (self.expires_on - 60)


# ============================================================
# Abstract base
# ============================================================


class AuthProvider(ABC):
    """Interface for all auth strategies."""

    @abstractmethod
    def get_token(self, scopes: List[str]) -> AccessToken:
        """Acquire or return a cached access token for the given scopes."""
        ...

    @abstractmethod
    def provider_type(self) -> str:
        """Human-readable provider type name."""
        ...


# ============================================================
# Token cache
# ============================================================


class TokenCache:
    """Thread-safe in-memory cache keyed by scope string."""

    def __init__(self) -> None:
        self._store: Dict[str, AccessToken] = {}
        self._lock = threading.Lock()

    def get(self, scope_key: str) -> Optional[AccessToken]:
        with self._lock:
            token = self._store.get(scope_key)
            if token is not None and not token.is_expired:
                return token
            # expired → remove
            self._store.pop(scope_key, None)
            return None

    def put(self, scope_key: str, token: AccessToken) -> None:
        with self._lock:
            self._store[scope_key] = token

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


# ============================================================
# Service Principal (OAuth2 client_credentials)
# ============================================================


class ServicePrincipalAuth(AuthProvider):
    """
    Authenticate using an Azure AD service principal (client_credentials flow).

    Requires ``azure-identity`` to be installed.
    """

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
    ) -> None:
        if not all([tenant_id, client_id, client_secret]):
            raise ConfigurationError(
                message="Service principal auth requires tenant_id, client_id, and client_secret.",
                action="Provide all three values in the connection auth config or environment variables.",
                context={
                    "tenant_id_set": bool(tenant_id),
                    "client_id_set": bool(client_id),
                    "client_secret_set": bool(client_secret),
                },
            )
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._cache = TokenCache()
        self._credential: Any = None  # lazy init

    def _ensure_credential(self) -> Any:
        if self._credential is not None:
            return self._credential
        try:
            from azure.identity import ClientSecretCredential
        except ImportError:
            raise ConfigurationError(
                message="azure-identity package is not installed.",
                action="Install cloud dependencies: pip install lizard-fpv[cloud]",
                context={},
            )
        self._credential = ClientSecretCredential(
            tenant_id=self._tenant_id,
            client_id=self._client_id,
            client_secret=self._client_secret,
        )
        return self._credential

    def get_token(self, scopes: List[str]) -> AccessToken:
        scope_key = " ".join(sorted(scopes))
        cached = self._cache.get(scope_key)
        if cached is not None:
            return cached

        credential = self._ensure_credential()
        try:
            azure_token = credential.get_token(*scopes)
        except Exception as exc:
            raise AuthenticationError(
                message=f"Failed to acquire token for scopes {scopes}: {exc}",
                action=(
                    "Verify your service principal credentials (tenant_id, client_id, client_secret). "
                    "Ensure the principal has the required RBAC roles on the target resource."
                ),
                context={"scopes": scopes, "error": str(exc)},
            ) from exc

        token = AccessToken(
            token=azure_token.token,
            expires_on=azure_token.expires_on,
            scope=scope_key,
        )
        self._cache.put(scope_key, token)
        return token

    def provider_type(self) -> str:
        return "service_principal"


# ============================================================
# Developer Token (PAT — for local testing)
# ============================================================


class DeveloperTokenAuth(AuthProvider):
    """
    Static token auth — wraps a Databricks Personal Access Token or any
    pre-issued bearer token.  Suitable only for local development / testing.
    """

    def __init__(self, token: str) -> None:
        if not token:
            raise ConfigurationError(
                message="Developer token auth requires a non-empty token.",
                action="Set the 'token' field in the auth configuration.",
                context={},
            )
        self._token = token

    def get_token(self, scopes: List[str]) -> AccessToken:
        # PATs don't have expiry info — set a far-future value
        return AccessToken(
            token=self._token,
            expires_on=time.time() + 86400 * 365,
            scope=" ".join(scopes),
        )

    def provider_type(self) -> str:
        return "developer_token"


# ============================================================
# Username + Password (where applicable)
# ============================================================


class UsernamePasswordAuth(AuthProvider):
    """
    Username/password auth via Azure AD.

    Only available when explicitly permitted by the Azure AD tenant
    configuration.  Not recommended for production.
    """

    def __init__(
        self, tenant_id: str, client_id: str, username: str, password: str
    ) -> None:
        if not all([tenant_id, client_id, username, password]):
            raise ConfigurationError(
                message="Username/password auth requires tenant_id, client_id, username, and password.",
                action="Provide all four values in the auth configuration.",
                context={},
            )
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._username = username
        self._password = password
        self._cache = TokenCache()
        self._credential: Any = None

    def _ensure_credential(self) -> Any:
        if self._credential is not None:
            return self._credential
        try:
            from azure.identity import UsernamePasswordCredential
        except ImportError:
            raise ConfigurationError(
                message="azure-identity package is not installed.",
                action="Install cloud dependencies: pip install lizard-fpv[cloud]",
                context={},
            )
        self._credential = UsernamePasswordCredential(
            tenant_id=self._tenant_id,
            client_id=self._client_id,
            username=self._username,
            password=self._password,
        )
        return self._credential

    def get_token(self, scopes: List[str]) -> AccessToken:
        scope_key = " ".join(sorted(scopes))
        cached = self._cache.get(scope_key)
        if cached is not None:
            return cached

        credential = self._ensure_credential()
        try:
            azure_token = credential.get_token(*scopes)
        except Exception as exc:
            raise AuthenticationError(
                message=f"Username/password authentication failed: {exc}",
                action="Verify username, password, tenant_id, and client_id.",
                context={"scopes": scopes, "error": str(exc)},
            ) from exc

        token = AccessToken(
            token=azure_token.token,
            expires_on=azure_token.expires_on,
            scope=scope_key,
        )
        self._cache.put(scope_key, token)
        return token

    def provider_type(self) -> str:
        return "username_password"


# ============================================================
# Factory
# ============================================================


def create_auth_provider(auth_config: AuthConfig) -> AuthProvider:
    """
    Build the appropriate ``AuthProvider`` from an ``AuthConfig``.

    Environment variables ``AZURE_TENANT_ID``, ``AZURE_CLIENT_ID``,
    ``AZURE_CLIENT_SECRET`` are used as fallbacks when the config fields
    are empty.
    """
    import os

    auth_type = auth_config.type

    if auth_type == "service_principal":
        return ServicePrincipalAuth(
            tenant_id=auth_config.tenant_id or os.environ.get("AZURE_TENANT_ID", ""),
            client_id=auth_config.client_id or os.environ.get("AZURE_CLIENT_ID", ""),
            client_secret=auth_config.client_secret
            or os.environ.get("AZURE_CLIENT_SECRET", ""),
        )

    if auth_type == "developer_token":
        return DeveloperTokenAuth(token=auth_config.token or "")

    if auth_type == "username_password":
        return UsernamePasswordAuth(
            tenant_id=auth_config.tenant_id or os.environ.get("AZURE_TENANT_ID", ""),
            client_id=auth_config.client_id or os.environ.get("AZURE_CLIENT_ID", ""),
            username=auth_config.username or "",
            password=auth_config.password or "",
        )

    raise ConfigurationError(
        message=f"Unknown auth type: '{auth_type}'.",
        action="Use one of: 'service_principal', 'developer_token', 'username_password'.",
        context={"auth_type": auth_type},
    )