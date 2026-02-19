# app/cloud_api.py
"""
Cloud Mode API — configuration, connectivity testing, and cloud data browsing.

Phase 1: config, test-connection
Phase 2: browse containers, browse blobs/DBFS, read cloud datasets
Phase 3: cluster management, execution engine control
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

log = logging.getLogger("lizard.cloud_api")

router = APIRouter(prefix="/cloud", tags=["Cloud Mode"])


# ============================================================
# Pydantic request / response models
# ============================================================

class ConfigResponse(BaseModel):
    mode: str = "local"
    gateways: List[Dict[str, Any]] = Field(default_factory=list)
    databricks_connections: List[Dict[str, Any]] = Field(default_factory=list)
    storage_connections: List[Dict[str, Any]] = Field(default_factory=list)


class ConfigUpdate(BaseModel):
    mode: Optional[str] = None
    gateways: Optional[List[Dict[str, Any]]] = None
    databricks_connections: Optional[List[Dict[str, Any]]] = None
    storage_connections: Optional[List[Dict[str, Any]]] = None


class ModeUpdate(BaseModel):
    mode: str  # "local" or "cloud"


class TestRequest(BaseModel):
    connection_type: str  # "databricks" or "storage"
    connection_name: str


class BrowseItem(BaseModel):
    name: str
    path: str
    provider: str
    size_bytes: Optional[int] = None
    last_modified: Optional[str] = None
    content_type: Optional[str] = None
    container: Optional[str] = None
    account: Optional[str] = None
    extra: Dict[str, Any] = Field(default_factory=dict)


class BrowseResponse(BaseModel):
    connection_name: str
    provider: str
    prefix: str
    items: List[BrowseItem]
    total: int


class DatasetPreviewResponse(BaseModel):
    connection_name: str
    path: str
    columns: List[str]
    row_count: int
    data: List[Dict[str, Any]]


# ============================================================
# Helpers — lazy-load cloud module
# ============================================================

def _get_config():
    """Import and return the current cloud config."""
    from cloud.config import get_config
    return get_config()


def _config_to_dict(cfg) -> Dict[str, Any]:
    """Serialize config for the API response, redacting secrets."""
    def _redact_auth(auth_dict: Dict[str, Any]) -> Dict[str, Any]:
        safe = dict(auth_dict)
        for secret_key in ("client_secret", "token", "password"):
            if safe.get(secret_key):
                safe[secret_key] = "••••••••"
        return safe

    result = {
        "mode": cfg.mode,
        "gateways": [gw.model_dump() for gw in cfg.gateways],
        "databricks_connections": [],
        "storage_connections": [],
    }
    for dc in cfg.databricks_connections:
        d = dc.model_dump()
        d["auth"] = _redact_auth(d.get("auth", {}))
        result["databricks_connections"].append(d)
    for sc in cfg.storage_connections:
        d = sc.model_dump()
        d["auth"] = _redact_auth(d.get("auth", {}))
        result["storage_connections"].append(d)
    return result


# ============================================================
# Config endpoints
# ============================================================

@router.get("/config")
async def get_cloud_config() -> Dict[str, Any]:
    """Return current cloud configuration (secrets redacted)."""
    try:
        cfg = _get_config()
        return _config_to_dict(cfg)
    except Exception as e:
        log.warning("cloud_config_error", error=str(e))
        return {"mode": "local", "gateways": [], "databricks_connections": [], "storage_connections": []}


@router.put("/config")
async def update_cloud_config(update: ConfigUpdate) -> Dict[str, Any]:
    """Update and persist cloud configuration."""
    from cloud.config import (
        LizardCloudConfig,
        GatewayConfig,
        DatabricksConnectionConfig,
        StorageConnectionConfig,
        set_config,
        save_config_to_yaml,
    )

    try:
        current = _get_config()
        data = current.model_dump()

        if update.mode is not None:
            data["mode"] = update.mode
        if update.gateways is not None:
            data["gateways"] = update.gateways
        if update.databricks_connections is not None:
            data["databricks_connections"] = update.databricks_connections
        if update.storage_connections is not None:
            data["storage_connections"] = update.storage_connections

        new_cfg = LizardCloudConfig(**data)
        set_config(new_cfg)

        import os
        yaml_path = os.getenv("LIZARD_CLOUD_CONFIG_PATH", "lizard-cloud.yaml")
        save_config_to_yaml(new_cfg, yaml_path)

        # Audit log
        from cloud.audit import record as audit_record
        audit_record(
            "config_update",
            category="config",
            detail={"fields_updated": [k for k in ["mode", "gateways", "databricks_connections", "storage_connections"] if getattr(update, k) is not None]},
        )

        return _config_to_dict(new_cfg)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/mode")
async def set_cloud_mode(body: ModeUpdate) -> Dict[str, str]:
    """Switch between local and cloud mode."""
    from cloud.config import get_config, set_config

    if body.mode not in ("local", "cloud"):
        raise HTTPException(status_code=400, detail="Mode must be 'local' or 'cloud'")

    cfg = get_config()
    updated = cfg.model_copy(update={"mode": body.mode})
    set_config(updated)

    from cloud.audit import record as audit_record
    audit_record(
        "mode_switch",
        category="config",
        detail={"old_mode": cfg.mode, "new_mode": updated.mode},
    )
    return {"mode": updated.mode}


# ============================================================
# Test connection
# ============================================================

@router.post("/test-connection")
async def test_connection(req: TestRequest) -> Dict[str, Any]:
    """Test connectivity to a Databricks workspace or storage account."""
    from cloud.config import get_config
    from cloud.connectivity import EndpointResolver, GatewayRegistry
    from cloud.auth import create_auth_provider
    from cloud.diagnostics import LizardCloudError

    cfg = get_config()
    steps: List[Dict[str, Any]] = []
    overall = "ok"

    try:
        registry = GatewayRegistry.from_config(cfg)
        resolver = EndpointResolver(registry)

        if req.connection_type == "databricks":
            conn = cfg.get_databricks_connection(req.connection_name)
            if not conn:
                return {
                    "connection_type": req.connection_type,
                    "connection_name": req.connection_name,
                    "overall": "error",
                    "steps": [],
                    "error": {"message": f"Databricks connection '{req.connection_name}' not found."},
                }

            # Step 1 — resolve endpoint
            try:
                host = resolver.resolve_databricks_host(
                    conn.workspace_id, conn.connectivity, conn.gateway_name
                )
                steps.append({"step": "resolve_endpoint", "status": "ok", "host": host})
            except LizardCloudError as e:
                steps.append({"step": "resolve_endpoint", "status": "error", "detail": e.to_dict()})
                from cloud.audit import record as audit_record
                audit_record(
                    "test_connection",
                    category="connection",
                    status=overall,
                    detail={
                        "connection_type": req.connection_type,
                        "connection_name": req.connection_name,
                        "steps_count": len(steps),
                    },
                )
                return _test_result(req, "error", steps)

            # Step 2 — authenticate
            try:
                auth = create_auth_provider(conn.auth)
                from cloud.constants import DATABRICKS_SCOPE
                token = auth.get_token([DATABRICKS_SCOPE])
                steps.append({"step": "authenticate", "status": "ok"})
            except Exception as e:
                steps.append({"step": "authenticate", "status": "error", "detail": {"message": str(e)}})
                return _test_result(req, "error", steps)

            # Step 3 — API ping
            try:
                import httpx
                async with httpx.AsyncClient(timeout=15, verify=False) as client:
                    resp = await client.get(
                        f"{host}/api/2.0/clusters/spark-versions",
                        headers={"Authorization": f"Bearer {token.token}"},
                    )
                    if resp.status_code < 400:
                        steps.append({"step": "api_ping", "status": "ok", "url": resp.url.path})
                    else:
                        steps.append({
                            "step": "api_ping",
                            "status": "error",
                            "detail": {"message": f"HTTP {resp.status_code}"},
                        })
                        overall = "partial"
            except Exception as e:
                steps.append({"step": "api_ping", "status": "error", "detail": {"message": str(e)}})
                overall = "partial"

        elif req.connection_type == "storage":
            conn = cfg.get_storage_connection(req.connection_name)
            if not conn:
                return {
                    "connection_type": req.connection_type,
                    "connection_name": req.connection_name,
                    "overall": "error",
                    "steps": [],
                    "error": {"message": f"Storage connection '{req.connection_name}' not found."},
                }

            # Step 1 — resolve endpoint
            try:
                url = resolver.resolve_storage_url(
                    conn.account_name, conn.connectivity, conn.gateway_name, conn.endpoint_type
                )
                steps.append({"step": "resolve_endpoint", "status": "ok", "url": url})
            except LizardCloudError as e:
                steps.append({"step": "resolve_endpoint", "status": "error", "detail": e.to_dict()})
                return _test_result(req, "error", steps)

            # Step 2 — authenticate
            try:
                auth = create_auth_provider(conn.auth)
                token = auth.get_token(["https://storage.azure.com/.default"])
                steps.append({"step": "authenticate", "status": "ok"})
            except Exception as e:
                steps.append({"step": "authenticate", "status": "error", "detail": {"message": str(e)}})
                return _test_result(req, "error", steps)

            # Step 3 — list containers
            try:
                from connectors.provider_factory import create_storage_provider
                provider = create_storage_provider(conn, cfg)
                containers = await provider.list_containers()
                steps.append({
                    "step": "list_containers",
                    "status": "ok",
                    "detail": {"count": len(containers)},
                })
            except NotImplementedError:
                steps.append({"step": "list_containers", "status": "skipped"})
            except Exception as e:
                steps.append({
                    "step": "list_containers",
                    "status": "error",
                    "detail": {"message": str(e)},
                })
                overall = "partial"

        else:
            raise HTTPException(status_code=400, detail="connection_type must be 'databricks' or 'storage'")

    except LizardCloudError as e:
        return {
            "connection_type": req.connection_type,
            "connection_name": req.connection_name,
            "overall": "error",
            "steps": steps,
            "error": e.to_dict(),
        }
    except Exception as e:
        return {
            "connection_type": req.connection_type,
            "connection_name": req.connection_name,
            "overall": "error",
            "steps": steps,
            "error": {"message": str(e)},
        }

    from cloud.audit import record as audit_record
    audit_record(
        "test_connection",
        category="connection",
        status=overall,
        detail={
            "connection_type": req.connection_type,
            "connection_name": req.connection_name,
            "steps_count": len(steps),
        },
    )

    return _test_result(req, overall, steps)


def _test_result(req: TestRequest, overall: str, steps: list) -> Dict[str, Any]:
    return {
        "connection_type": req.connection_type,
        "connection_name": req.connection_name,
        "overall": overall,
        "steps": steps,
    }


# ============================================================
# Phase 2: Cloud data browsing
# ============================================================

@router.get("/browse/storage/{connection_name}/containers")
async def browse_storage_containers(connection_name: str) -> Dict[str, Any]:
    """List containers in a storage account."""
    from cloud.config import get_config
    from connectors.provider_factory import create_storage_provider

    cfg = get_config()
    conn = cfg.get_storage_connection(connection_name)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Storage connection '{connection_name}' not found.")

    try:
        provider = create_storage_provider(conn, cfg)
        containers = await provider.list_containers()
        return {
            "connection_name": connection_name,
            "account": conn.account_name,
            "containers": containers,
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/browse/storage/{connection_name}/blobs")
async def browse_storage_blobs(
    connection_name: str,
    container: str = Query(default="", description="Container name (overrides connection default)"),
    prefix: str = Query(default="", description="Blob path prefix"),
    search: str = Query(default="", description="Search substring"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:
    """List blobs in a storage container."""
    from cloud.config import get_config
    from connectors.provider_factory import create_storage_provider

    cfg = get_config()
    conn = cfg.get_storage_connection(connection_name)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Storage connection '{connection_name}' not found.")

    try:
        provider = create_storage_provider(conn, cfg)
        target_container = container or conn.container
        items = await provider.list_sources(
            prefix=target_container,
            limit=limit,
            offset=offset,
            search=search or None,
        )
        return {
            "connection_name": connection_name,
            "provider": provider.provider_name(),
            "container": target_container,
            "prefix": prefix,
            "items": [item.to_dict() for item in items],
            "total": len(items),
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/browse/dbfs/{connection_name}")
async def browse_dbfs(
    connection_name: str,
    path: str = Query(default="/", description="DBFS directory path"),
    search: str = Query(default="", description="Search substring"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:
    """List files in a Databricks DBFS directory."""
    from cloud.config import get_config
    from connectors.provider_factory import create_dbfs_provider

    cfg = get_config()
    conn = cfg.get_databricks_connection(connection_name)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Databricks connection '{connection_name}' not found.")

    try:
        provider = create_dbfs_provider(conn, cfg)
        items = await provider.list_sources(
            prefix=path,
            limit=limit,
            offset=offset,
            search=search or None,
        )
        return {
            "connection_name": connection_name,
            "provider": provider.provider_name(),
            "path": path,
            "items": [item.to_dict() for item in items],
            "total": len(items),
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/browse/preview")
async def preview_cloud_dataset(
    connection_name: str = Query(..., description="Connection name"),
    connection_type: str = Query(..., description="'storage' or 'dbfs'"),
    path: str = Query(..., description="Full dataset path"),
    format: str = Query(default="", description="csv, parquet, json (auto-detected if empty)"),
    limit: int = Query(default=100, ge=1, le=10000),
) -> Dict[str, Any]:
    """Preview a cloud dataset (read into DataFrame and return first N rows)."""
    from cloud.config import get_config
    from connectors.provider_factory import create_storage_provider, create_dbfs_provider

    cfg = get_config()

    try:
        if connection_type == "storage":
            conn = cfg.get_storage_connection(connection_name)
            if not conn:
                raise HTTPException(status_code=404, detail=f"Storage connection '{connection_name}' not found.")
            provider = create_storage_provider(conn, cfg)
        elif connection_type == "dbfs":
            conn = cfg.get_databricks_connection(connection_name)
            if not conn:
                raise HTTPException(status_code=404, detail=f"Databricks connection '{connection_name}' not found.")
            provider = create_dbfs_provider(conn, cfg)
        else:
            raise HTTPException(status_code=400, detail="connection_type must be 'storage' or 'dbfs'")

        import numpy as np
        from datetime import datetime as dt
        import pandas as pd

        df = await provider.read_dataset(
            path=path,
            format=format or None,
            limit=limit,
        )

        records = df.replace({np.nan: None}).to_dict(orient="records")

        # Serialize datetime / numpy types
        for record in records:
            for key, value in record.items():
                if isinstance(value, (dt, pd.Timestamp)):
                    record[key] = value.isoformat()
                elif hasattr(value, "item"):
                    record[key] = value.item()

        return {
            "connection_name": connection_name,
            "path": path,
            "columns": list(df.columns),
            "row_count": len(records),
            "data": records,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ============================================================
# Provider registry (for use by workbench / other modules)
# ============================================================

@router.get("/providers")
async def list_providers() -> Dict[str, Any]:
    """List all configured data-source providers and their status."""
    from cloud.config import get_config

    cfg = get_config()
    providers: List[Dict[str, Any]] = []

    # Local provider is always available
    providers.append({
        "name": "local",
        "type": "local",
        "status": "available",
    })

    if cfg.is_cloud_mode:
        for sc in cfg.storage_connections:
            providers.append({
                "name": sc.name,
                "type": f"azure_{sc.endpoint_type}",
                "account": sc.account_name,
                "container": sc.container,
                "connectivity": sc.connectivity,
                "status": "configured",
            })
        for dc in cfg.databricks_connections:
            providers.append({
                "name": dc.name,
                "type": "dbfs",
                "workspace_id": dc.workspace_id,
                "connectivity": dc.connectivity,
                "status": "configured",
            })

    return {"mode": cfg.mode, "providers": providers}


# ============================================================
# Cloud Import: Analyze + Ingest
# Add these to app/cloud_api.py after the preview_cloud_dataset endpoint
# ============================================================


class CloudIngestRequest(BaseModel):
    connection_name: str
    connection_type: str  # "storage" or "dbfs"
    path: str
    source_name: str
    mapping_json: Dict[str, Any]
    template_id: Optional[str] = None


@router.post("/analyze")
async def analyze_cloud_source(
    connection_name: str = Query(..., description="Connection name"),
    connection_type: str = Query(..., description="'storage' or 'dbfs'"),
    path: str = Query(..., description="Full dataset path"),
    engine: str = Query(default="heuristic", description="Mapping engine"),
    limit: int = Query(default=5000, ge=100, le=100000),
) -> Dict[str, Any]:
    """
    Analyze a cloud dataset: read it, detect columns, and suggest a mapping.

    This is the cloud equivalent of POST /mapping/templates/suggest.
    It reads the dataset from the cloud provider, then runs the same
    mapping suggestion engine on the resulting DataFrame.
    """
    from cloud.config import get_config
    from connectors.provider_factory import create_storage_provider, create_dbfs_provider

    cfg = get_config()

    # 1. Read the dataset from cloud
    try:
        if connection_type == "storage":
            conn = cfg.get_storage_connection(connection_name)
            if not conn:
                raise HTTPException(status_code=404, detail=f"Storage connection '{connection_name}' not found.")
            provider = create_storage_provider(conn, cfg)
        elif connection_type == "dbfs":
            conn = cfg.get_databricks_connection(connection_name)
            if not conn:
                raise HTTPException(status_code=404, detail=f"Databricks connection '{connection_name}' not found.")
            provider = create_dbfs_provider(conn, cfg)
        else:
            raise HTTPException(status_code=400, detail="connection_type must be 'storage' or 'dbfs'")

        df = await provider.read_dataset(path=path, format=None, limit=limit)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to read cloud dataset: {e}")

    # 2. Run the mapping suggestion engine on the DataFrame
    #    Uses the REAL functions from mapping/ai_mapper.py
    try:
        from mapping.ai_mapper import (
            suggest_event_mapping,
            suggest_mapping_with_scores,
            analyze_columns,
        )

        sample = df.head(100)

        mapping = suggest_event_mapping(sample, engine=engine)
        expressions = mapping.pop("__expr__", {})
        candidates = suggest_mapping_with_scores(sample)
        column_analysis = analyze_columns(sample)

        filename = path.split("/")[-1] if "/" in path else path

        return {
            "filename": filename,
            "total_rows": len(df),
            "columns": list(df.columns),
            "suggested_mapping": mapping,
            "suggested_expressions": expressions,
            "candidates": candidates,
            "column_analysis": column_analysis,
            "engine_used": engine,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Mapping analysis failed: {e}")


@router.post("/ingest")
async def ingest_cloud_dataset(request: CloudIngestRequest) -> Dict[str, Any]:
    """
    Read a cloud dataset and ingest it into the local event store
    using the provided mapping — the cloud equivalent of POST /upload/events.
    """
    from cloud.config import get_config
    from connectors.provider_factory import create_storage_provider, create_dbfs_provider

    cfg = get_config()

    # 1. Read the dataset
    try:
        if request.connection_type == "storage":
            conn = cfg.get_storage_connection(request.connection_name)
            if not conn:
                raise HTTPException(status_code=404, detail=f"Storage connection '{request.connection_name}' not found.")
            provider = create_storage_provider(conn, cfg)
        elif request.connection_type == "dbfs":
            conn = cfg.get_databricks_connection(request.connection_name)
            if not conn:
                raise HTTPException(status_code=404, detail=f"Databricks connection '{request.connection_name}' not found.")
            provider = create_dbfs_provider(conn, cfg)
        else:
            raise HTTPException(status_code=400, detail="connection_type must be 'storage' or 'dbfs'")

        df = await provider.read_dataset(path=request.path, format=None, limit=None)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to read cloud dataset: {e}")

    # 2. Apply mapping and ingest using the async adapter
    try:
        from app.ingest_engine import async_apply_mapping_and_ingest

        mapping = dict(request.mapping_json)
        # Separate expressions from field mapping
        expressions = mapping.pop("__expr__", {})
        field_mapping = {k: v for k, v in mapping.items() if not k.startswith("_")}

        result = await async_apply_mapping_and_ingest(
            df=df,
            field_mapping=field_mapping,
            expressions=expressions,
            source_name=request.source_name,
        )

        # Audit log
        try:
            from cloud.audit import record as audit_record
            audit_record(
                "cloud_ingest",
                category="ingest",
                detail={
                    "connection_name": request.connection_name,
                    "connection_type": request.connection_type,
                    "path": request.path,
                    "source_name": request.source_name,
                    "rows_ingested": result.get("ingested", 0),
                },
            )
        except Exception:
            log.warning("audit_record_failed_for_cloud_ingest")

        return {
            "ingested": result.get("ingested", 0),
            "rejected": result.get("rejected", 0),
            "source_name": request.source_name,
            "mapping_used": field_mapping,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {e}")

# ============================================================
# Phase 3: Cluster Management Endpoints
# ============================================================

@router.get("/clusters/{connection_name}")
async def list_clusters(connection_name: str) -> Dict[str, Any]:
    """List all Databricks clusters for a connection."""
    from cloud.cluster_manager import list_clusters as _list

    try:
        clusters = await _list(connection_name)
        return {"connection": connection_name, "clusters": clusters}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/clusters/{connection_name}/{cluster_id}")
async def get_cluster_status(connection_name: str, cluster_id: str) -> Dict[str, Any]:
    """Get status of a specific Databricks cluster."""
    from cloud.cluster_manager import get_cluster_status as _status

    try:
        status = await _status(connection_name, cluster_id)
        return status
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/clusters/{connection_name}/{cluster_id}/start")
async def start_cluster(connection_name: str, cluster_id: str) -> Dict[str, Any]:
    """Start a Databricks cluster."""
    from cloud.cluster_manager import start_cluster as _start

    try:
        result = await _start(connection_name, cluster_id)
        return result
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/clusters/{connection_name}/{cluster_id}/stop")
async def stop_cluster(connection_name: str, cluster_id: str) -> Dict[str, Any]:
    """Stop a Databricks cluster."""
    from cloud.cluster_manager import stop_cluster as _stop

    try:
        result = await _stop(connection_name, cluster_id)
        return result
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/warehouses/{connection_name}")
async def list_warehouses(connection_name: str) -> Dict[str, Any]:
    """List all SQL Warehouses for a connection."""
    from cloud.cluster_manager import list_warehouses as _list_wh

    try:
        warehouses = await _list_wh(connection_name)
        return {"connection": connection_name, "warehouses": warehouses}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


# ============================================================
# Phase 3: Execution Engine Endpoints
# ============================================================

@router.get("/engine/status")
async def engine_status() -> Dict[str, Any]:
    """
    Get the current execution engine status.
    Returns which engine is active and its health.
    """
    from cloud.execution.engine_factory import get_engine

    try:
        eng = get_engine()
        health = await eng.health_check()
        return health
    except Exception as exc:
        return {"engine": "unknown", "status": "error", "error": str(exc)}


@router.post("/engine/reset")
async def reset_engine() -> Dict[str, Any]:
    """
    Reset cached execution engines.
    Call this after changing cloud config (connection, cluster, etc.).
    """
    from cloud.execution.engine_factory import reset_engines

    reset_engines()
    return {"status": "ok", "message": "Execution engines reset. Next request will re-initialize."}


class SetComputeRequest(BaseModel):
    connection_name: str
    cluster_id: Optional[str] = None
    warehouse_id: Optional[str] = None


@router.post("/engine/set-compute")
async def set_compute(request: SetComputeRequest) -> Dict[str, Any]:
    """
    Set which cluster or warehouse to use for Spark execution.
    Updates the Databricks connection config and resets the engine.
    """
    from cloud.config import get_config, save_config
    from cloud.execution.engine_factory import reset_engines

    cfg = get_config()
    conn = cfg.get_databricks_connection(request.connection_name)
    if conn is None:
        raise HTTPException(
            status_code=404,
            detail=f"Databricks connection '{request.connection_name}' not found.",
        )

    # Update the connection's compute settings
    conn.cluster_id = request.cluster_id
    conn.warehouse_id = request.warehouse_id

    # Persist and reset engines
    save_config(cfg)
    reset_engines()

    return {
        "status": "ok",
        "connection": request.connection_name,
        "cluster_id": request.cluster_id,
        "warehouse_id": request.warehouse_id,
        "message": "Compute target updated. Engine will reinitialize on next request.",
    }

# ============================================================
# Phase 4: Export / Output Storage Endpoints
# ============================================================

class ExportRequest(BaseModel):
    pipeline: List[Dict[str, Any]]
    connection_name: str
    connection_type: str = "storage"  # "storage" or "dbfs"
    container: Optional[str] = None
    path_prefix: str = "lizard-exports"
    filename: Optional[str] = None
    format: str = "parquet"  # "csv" or "parquet"
    limit: int = 100000


@router.post("/export")
async def export_results(request: ExportRequest) -> Dict[str, Any]:
    """
    Execute a pipeline and export the results to cloud storage.

    Supports exporting to Azure Blob Storage or Databricks DBFS.
    """
    from cloud.execution.engine_factory import get_engine
    from cloud.output_storage import export_to_blob_storage, export_to_dbfs

    # Step 1: Execute pipeline to get data
    try:
        eng = get_engine()
        result = await eng.execute_pipeline(
            request.pipeline, limit=request.limit, offset=0
        )
        df = result.data
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Pipeline execution failed: {exc}")

    if df.empty:
        raise HTTPException(status_code=400, detail="Pipeline returned no data to export.")

    # Step 2: Export to cloud storage
    try:
        if request.connection_type == "storage":
            export_result = await export_to_blob_storage(
                df=df,
                connection_name=request.connection_name,
                container=request.container,
                path_prefix=request.path_prefix,
                filename=request.filename,
                format=request.format,
            )
        elif request.connection_type == "dbfs":
            export_result = await export_to_dbfs(
                df=df,
                connection_name=request.connection_name,
                path_prefix=request.path_prefix,
                filename=request.filename,
                format=request.format,
            )
        else:
            raise HTTPException(
                status_code=400,
                detail="connection_type must be 'storage' or 'dbfs'",
            )

        export_result["engine"] = result.engine
        export_result["execution_time_ms"] = result.execution_time_ms

        from cloud.audit import record as audit_record
        audit_record(
            "export",
            category="export",
            detail={
                "connection_type": request.connection_type,
                "connection_name": request.connection_name,
                "format": request.format,
                "rows": export_result.get("rows", 0),
            },
        )

        return export_result

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Export failed: {exc}")

# ============================================================
# Phase 4: Cloud Analytics Endpoints
# ============================================================

class AnomalyRequest(BaseModel):
    pipeline: List[Dict[str, Any]]
    method: str = "simple"  # "simple" or "advanced"
    limit: int = 50000
    # Simple params
    feature: str = "count"
    z_thr: float = 3.0
    bucket: str = "1m"
    alpha: float = 0.2
    # Advanced params
    contamination: float = 0.05
    speed_kmh_thr: float = 900.0
    dist_km_thr: float = 2000.0
    event_velocity_thr: float = 10.0
    rare_hour_prob: float = 0.05


class ClusteringRequest(BaseModel):
    pipeline: List[Dict[str, Any]]
    limit: int = 50000
    eps: float = 0.7
    min_samples: int = 15


@router.post("/analytics/anomaly")
async def run_anomaly(request: AnomalyRequest) -> Dict[str, Any]:
    """
    Run anomaly detection on pipeline results.

    Supports 'simple' (EWMA + MAD z-score) and 'advanced' (Isolation Forest).
    Uses the execution engine to fetch data, then applies analytics.
    """
    from cloud.analytics_engine import run_anomaly_detection

    try:
        result = await run_anomaly_detection(
            pipeline=request.pipeline,
            method=request.method,
            limit=request.limit,
            feature=request.feature,
            z_thr=request.z_thr,
            bucket=request.bucket,
            alpha=request.alpha,
            contamination=request.contamination,
            speed_kmh_thr=request.speed_kmh_thr,
            dist_km_thr=request.dist_km_thr,
            event_velocity_thr=request.event_velocity_thr,
            rare_hour_prob=request.rare_hour_prob,
        )

        from cloud.audit import record as audit_record
        audit_record(
            "analytics_anomaly",
            category="analytics",
            detail={
                "method": request.method,
                "total_rows": result.get("total_rows", 0),
                "anomaly_count": result.get("anomaly_count", 0),
            },
            duration_ms=result.get("execution_time_ms"),
        )

        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Anomaly detection failed: {exc}")


@router.post("/analytics/clustering")
async def run_clustering_endpoint(request: ClusteringRequest) -> Dict[str, Any]:
    """
    Run geo-temporal clustering (DBSCAN) on pipeline results.

    Uses the execution engine to fetch data, then applies DBSCAN clustering
    on lat/lon/time features.
    """
    from cloud.analytics_engine import run_clustering

    try:
        result = await run_clustering(
            pipeline=request.pipeline,
            limit=request.limit,
            eps=request.eps,
            min_samples=request.min_samples,
        )

        from cloud.audit import record as audit_record
        audit_record(
            "analytics_clustering",
            category="analytics",
            detail={
                "total_rows": result.get("total_rows", 0),
                "num_clusters": result.get("num_clusters", 0),
            },
            duration_ms=result.get("execution_time_ms"),
        )

        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Clustering failed: {exc}")

# ============================================================
# Phase 5: Audit Logging & Health Dashboard
# ============================================================


# ── Audit API endpoints ──────────────────────────────────────────────

@router.get("/audit")
async def get_audit_log(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    category: Optional[str] = Query(default=None, description="Filter by category"),
    status: Optional[str] = Query(default=None, description="Filter by status"),
    operation: Optional[str] = Query(default=None, description="Filter by operation"),
) -> Dict[str, Any]:
    """Retrieve cloud audit log entries (newest first)."""
    from cloud.audit import get_entries, get_stats

    entries = get_entries(
        limit=limit,
        offset=offset,
        category=category,
        status=status,
        operation=operation,
    )

    return {
        "entries": [e.model_dump() for e in entries],
        "count": len(entries),
        "offset": offset,
        "limit": limit,
        "filters": {
            "category": category,
            "status": status,
            "operation": operation,
        },
    }


@router.get("/audit/stats")
async def get_audit_stats() -> Dict[str, Any]:
    """Return audit log summary statistics."""
    from cloud.audit import get_stats

    return get_stats()


@router.delete("/audit")
async def clear_audit_log() -> Dict[str, Any]:
    """Clear all audit log entries."""
    from cloud.audit import clear

    count = clear()
    return {"cleared": count}


# ── Health Dashboard ─────────────────────────────────────────────────

@router.get("/health")
async def cloud_health() -> Dict[str, Any]:
    """
    Comprehensive cloud health check.

    Checks:
      - Cloud mode status
      - All Databricks connection reachability
      - All storage connection reachability
      - Execution engine status
      - Audit log stats
    """
    from cloud.config import get_config
    from cloud.audit import get_stats as audit_stats
    import time

    t0 = time.perf_counter()
    cfg = get_config()

    checks: List[Dict[str, Any]] = []
    overall = "healthy"

    # Check 1: Cloud mode
    checks.append({
        "check": "cloud_mode",
        "status": "ok",
        "detail": {"mode": cfg.mode},
    })

    # Check 2: Databricks connections
    for dc in cfg.databricks_connections:
        try:
            from cloud.connectivity import GatewayRegistry, EndpointResolver

            registry = GatewayRegistry.from_config(cfg)
            resolver = EndpointResolver(registry)
            host = resolver.resolve_databricks_host(
                dc.workspace_id, dc.connectivity, dc.gateway_name
            )
            checks.append({
                "check": f"databricks:{dc.name}",
                "status": "ok",
                "detail": {"host": host, "connectivity": dc.connectivity},
            })
        except Exception as e:
            checks.append({
                "check": f"databricks:{dc.name}",
                "status": "error",
                "detail": {"error": str(e)},
            })
            overall = "degraded"

    # Check 3: Storage connections
    for sc in cfg.storage_connections:
        try:
            from cloud.connectivity import GatewayRegistry, EndpointResolver

            registry = GatewayRegistry.from_config(cfg)
            resolver = EndpointResolver(registry)
            url = resolver.resolve_storage_url(
                sc.account_name, sc.connectivity, sc.gateway_name, sc.endpoint_type
            )
            checks.append({
                "check": f"storage:{sc.name}",
                "status": "ok",
                "detail": {"url": url, "connectivity": sc.connectivity},
            })
        except Exception as e:
            checks.append({
                "check": f"storage:{sc.name}",
                "status": "error",
                "detail": {"error": str(e)},
            })
            overall = "degraded"

    # Check 4: Execution engine
    try:
        from cloud.execution.engine_factory import get_engine

        eng = get_engine()
        checks.append({
            "check": "execution_engine",
            "status": "ok",
            "detail": {"engine": eng.engine_name()},
        })
    except Exception as e:
        checks.append({
            "check": "execution_engine",
            "status": "error",
            "detail": {"error": str(e)},
        })
        # Engine failure in local mode is not critical
        if cfg.is_cloud_mode:
            overall = "degraded"

    # Check 5: Audit log
    a_stats = audit_stats()
    checks.append({
        "check": "audit_log",
        "status": "ok",
        "detail": {
            "total_entries": a_stats["total_entries"],
            "error_count": a_stats.get("by_status", {}).get("error", 0),
        },
    })

    elapsed_ms = (time.perf_counter() - t0) * 1000

    # Record this health check in audit
    from cloud.audit import record
    record(
        "health_check",
        category="system",
        status=overall,
        detail={"checks_count": len(checks), "overall": overall},
        duration_ms=round(elapsed_ms, 1),
    )

    return {
        "status": overall,
        "mode": cfg.mode,
        "checks": checks,
        "elapsed_ms": round(elapsed_ms, 1),
        "databricks_connections": len(cfg.databricks_connections),
        "storage_connections": len(cfg.storage_connections),
        "gateways": len(cfg.gateways),
    }


# ── Cloud status summary (lightweight) ───────────────────────────────

@router.get("/status")
async def cloud_status() -> Dict[str, Any]:
    """Lightweight cloud status (no connectivity checks)."""
    from cloud.config import get_config
    from cloud.audit import get_stats

    cfg = get_config()
    stats = get_stats()

    return {
        "mode": cfg.mode,
        "databricks_connections": len(cfg.databricks_connections),
        "storage_connections": len(cfg.storage_connections),
        "gateways": len(cfg.gateways),
        "audit": {
            "total_entries": stats["total_entries"],
            "errors": stats.get("by_status", {}).get("error", 0),
        },
    }

# ============================================================
# Phase 6: Scheduler endpoints
# ============================================================


@router.get("/scheduler")
async def get_scheduler_status() -> Dict[str, Any]:
    """Get scheduler status and all registered jobs."""
    from cloud.scheduler import get_scheduler

    s = get_scheduler()
    return {
        "running": s.is_running,
        "jobs": [j.to_dict() for j in s.list_jobs()],
        "job_count": len(s.list_jobs()),
    }


@router.post("/scheduler/start")
async def start_scheduler() -> Dict[str, str]:
    """Start the background scheduler with default jobs."""
    from cloud.scheduler import get_scheduler, register_default_jobs

    s = get_scheduler()
    if not s.list_jobs():
        register_default_jobs(s)
    s.start()
    return {"status": "started"}


@router.post("/scheduler/stop")
async def stop_scheduler() -> Dict[str, str]:
    """Stop the background scheduler."""
    from cloud.scheduler import get_scheduler

    s = get_scheduler()
    s.stop()
    return {"status": "stopped"}


@router.post("/scheduler/jobs/{job_name}/run")
async def run_job_now(job_name: str) -> Dict[str, Any]:
    """Trigger a scheduled job immediately."""
    from cloud.scheduler import get_scheduler

    s = get_scheduler()
    result = s.run_now(job_name)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found.")
    return result


@router.post("/scheduler/jobs/{job_name}/enable")
async def enable_job(job_name: str) -> Dict[str, str]:
    """Enable a scheduled job."""
    from cloud.scheduler import get_scheduler

    s = get_scheduler()
    if s.enable_job(job_name):
        return {"status": "enabled", "job": job_name}
    raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found.")


@router.post("/scheduler/jobs/{job_name}/disable")
async def disable_job(job_name: str) -> Dict[str, str]:
    """Disable a scheduled job."""
    from cloud.scheduler import get_scheduler

    s = get_scheduler()
    if s.disable_job(job_name):
        return {"status": "disabled", "job": job_name}
    raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found.")