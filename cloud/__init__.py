# cloud/__init__.py
"""
LIZARD Cloud Mode package.

Provides connectivity to Azure Blob Storage, ADLS Gen2, and Databricks
with support for direct and Application Gateway routing.

All Azure/Databricks SDK imports are lazy — this package is safe to import
even when cloud dependencies are not installed.
"""
from __future__ import annotations

import importlib

# ---------------------------------------------------------------------------
# Feature flag: are cloud dependencies installed?
# ---------------------------------------------------------------------------

def _check_cloud_deps() -> bool:
    """Return True if the optional cloud dependencies are available."""
    for mod in ("azure.identity", "azure.storage.blob"):
        try:
            importlib.import_module(mod)
        except ImportError:
            return False
    return True


CLOUD_DEPS_AVAILABLE: bool = _check_cloud_deps()

__all__ = [
    "CLOUD_DEPS_AVAILABLE",
]