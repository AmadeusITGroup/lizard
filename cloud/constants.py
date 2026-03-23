# cloud/constants.py
"""
Constants for Azure and Databricks cloud connectivity.
"""
from __future__ import annotations

# ============================================================
# Azure AD / Entra ID OAuth scopes
# ============================================================

# Scope for Azure Databricks API calls
DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"

# Scope for Azure Storage (Blob / ADLS Gen2) data-plane calls
STORAGE_SCOPE = "https://storage.azure.com/.default"

# Scope for Azure Management plane (listing resources, etc.)
MANAGEMENT_SCOPE = "https://management.azure.com/.default"

# ============================================================
# Endpoint URL templates
# ============================================================

# Direct Databricks workspace URL template
# {workspace_id} is the numeric Azure Databricks workspace ID
DATABRICKS_DIRECT_HOST_TEMPLATE = "https://{gateway_fqdn}/databricks/{workspace_id}"

# Direct Azure Blob Storage endpoint
# {account_name} is the storage account name
BLOB_DIRECT_ENDPOINT_TEMPLATE = "https://{account_name}.blob.core.windows.net"

# Direct Azure Data Lake Storage Gen2 (DFS) endpoint
DFS_DIRECT_ENDPOINT_TEMPLATE = "https://{account_name}.dfs.core.windows.net"

# ============================================================
# Gateway URL templates
# ============================================================

# Databricks via Application Gateway
# The gateway rewrites the host based on workspace ID.
# {gateway_fqdn} is the gateway's fully-qualified domain name.
# {workspace_id} is the numeric workspace identifier.
DATABRICKS_GATEWAY_HOST_TEMPLATE = "https://{gateway_fqdn}/databricks/{workspace_id}"

# Azure Blob Storage via Application Gateway (SDK base-URL rewrite)
# Storage requests are routed through the gateway using:
#   https://{gateway_fqdn}/{account_name}.blob.core.windows.net
BLOB_GATEWAY_ENDPOINT_TEMPLATE = (
    "https://{gateway_fqdn}/{account_name}.blob.core.windows.net"
)

# ADLS Gen2 (DFS) via Application Gateway
DFS_GATEWAY_ENDPOINT_TEMPLATE = (
    "https://{gateway_fqdn}/{account_name}.dfs.core.windows.net"
)

# ============================================================
# Supported connectivity strategies
# ============================================================

CONNECTIVITY_DIRECT = "direct"
CONNECTIVITY_GATEWAY = "gateway"
VALID_CONNECTIVITY_MODES = {CONNECTIVITY_DIRECT, CONNECTIVITY_GATEWAY}

# ============================================================
# Supported storage endpoint types
# ============================================================

ENDPOINT_BLOB = "blob"
ENDPOINT_DFS = "dfs"
VALID_ENDPOINT_TYPES = {ENDPOINT_BLOB, ENDPOINT_DFS}

# ============================================================
# Default configuration file name
# ============================================================

DEFAULT_CONFIG_FILENAME = "lizard-cloud.yaml"