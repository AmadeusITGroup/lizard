# path: app/main.py
from __future__ import annotations
import io
import json
import os
import time
import zipfile
import math
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Literal, Union, Iterable
from datetime import datetime
import uuid

import numpy as np
import orjson
import pandas as pd
import structlog
from fastapi import (
    Body,
    Depends,
    FastAPI,
    File,
    Form,
    HTTPException,
    Query,
    Response,
    UploadFile,
)
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

# ── analytics ────────────────────────────────────────────────────────
from analytics.advanced_anomaly import compute_advanced_anomaly
from analytics.clustering import cluster_geo_temporal
from analytics.simple_anomaly import mark_anomalies

# ── domain & mapping ─────────────────────────────────────────────────
from domain.models import Base, Entity, Event, Link, make_engine_and_session
from domain.schemas import EntitiesIn, EventIn, EventOut, GlobeQuery
from mapping.ai_mapper import suggest_event_mapping
from mapping.expr import apply_mapping_with_expr
from connectors.csv.loader import events_from_csv_bytes, _build_event_payload  # noqa

try:
    import networkx as nx
    from networkx.algorithms.community import greedy_modularity_communities, asyn_lpa_communities
    HAVE_NX = True
except Exception:
    HAVE_NX = False

log = structlog.get_logger(__name__)
app = FastAPI(title="LIZARD API", version="4.3.0")

# ── CORS ─────────────────────────────────────────────────────────────
ALLOWED_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:8501",
    "http://127.0.0.1:8501",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Prometheus metrics ───────────────────────────────────────────────
ingested_events = Counter("lizard_ingested_events_total", "Total events ingested")
query_latency = Histogram(
    "lizard_query_seconds",
    "Query duration seconds",
    buckets=(0.05, 0.1, 0.2, 0.4, 0.8, 2, 5, 10),
)

# ── database ─────────────────────────────────────────────────────────
DB_URL = os.getenv("LIZARD_DB_URL", "sqlite+aiosqlite:///./lizard.db")
engine, SessionLocal = make_engine_and_session(DB_URL)


async def get_session() -> AsyncSession:
    async with SessionLocal() as session:
        yield session


@app.on_event("startup")
async def on_startup() -> None:
    log.info("starting_up", db=DB_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# ── health & metrics ─────────────────────────────────────────────────

@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ══════════════════════════════════════════════════════════════════════
# Helpers: ISO parsing, data fetching, JSON sanitisation
# ══════════════════════════════════════════════════════════════════════

def _parse_iso(ts: str) -> datetime:
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid ISO datetime: {ts}") from exc


# ── engine-aware data fetch (public dispatcher) ──────────────────────

async def _events_df(
    session: AsyncSession,
    start: str,
    end: str,
    limit: int = 50_000,
    source: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch events as a DataFrame.

    In cloud mode the request is forwarded to the Spark execution engine
    via app.viz_engine so that all computation happens on the remote
    Databricks cluster. In local mode the existing SQLAlchemy path is
    used with zero behaviour change.
    """
    from app.viz_engine import _is_cloud_mode
    if _is_cloud_mode():
        from app.viz_engine import get_viz_data_raw
        return await get_viz_data_raw(start=start, end=end, limit=limit, source=source)
    return await _events_df_local(session, start, end, limit=limit, source=source)


async def _events_df_local(
    session: AsyncSession,
    start: str,
    end: str,
    limit: int = 50_000,
    source: Optional[str] = None,
) -> pd.DataFrame:
    """Local-only data fetch via SQLAlchemy (unchanged legacy path)."""
    sdt, edt = _parse_iso(start), _parse_iso(end)

    if source:
        try:
            from app.workbench_api import _derived_views, _materialized_data, get_source_data

            if source in _materialized_data:
                df = _materialized_data[source].copy()
                if "ts" in df.columns:
                    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
                    df = df[(df["ts"] >= sdt) & (df["ts"] <= edt)]
                return df.head(limit)

            for _vid, view in _derived_views.items():
                if view["name"] == source:
                    from app.workbench_api import PipelineExecutor, PipelineStep
                    executor = PipelineExecutor(get_source_data)
                    pipeline = [PipelineStep(**s) for s in view["pipeline"]]
                    df, _ = await executor.execute(pipeline, limit=limit, offset=0)
                    if "ts" in df.columns:
                        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
                        df = df[(df["ts"] >= sdt) & (df["ts"] <= edt)]
                    return df
        except ImportError:
            pass
        except Exception as e:
            log.warning("view_execution_failed", source=source, error=str(e))

    stmt = select(Event).where(Event.ts >= sdt, Event.ts <= edt)
    if source:
        stmt = stmt.where(Event.source == source)
    stmt = stmt.order_by(Event.ts).limit(limit)

    rows = (await session.scalars(stmt)).all()
    if not rows:
        return pd.DataFrame(columns=["ts"])

    data = [e.to_out().model_dump() for e in rows]
    df = pd.DataFrame(data)
    if "ts" in df:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    return df


# ── JSON sanitisation ────────────────────────────────────────────────

def _to_iso_utc(ts: pd.Timestamp | datetime | None) -> str | None:
    if ts is None:
        return None
    if isinstance(ts, pd.Timestamp):
        if pd.isna(ts):
            return None
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    return None


def _df_to_json_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    d = df.copy()
    for c in d.columns:
        if pd.api.types.is_datetime64_any_dtype(d[c]):
            try:
                d[c] = pd.to_datetime(d[c], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                d[c] = d[c].astype(str)
    d = d.where(pd.notnull(d), None)
    return d.to_dict(orient="records")


def _json_sanitize(obj):
    if obj is pd.NaT:
        return None
    if isinstance(obj, pd.Timestamp):
        return _to_iso_utc(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        v = float(obj)
        return None if (math.isnan(v) or math.isinf(v)) else v
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, dict):
        return {str(k): _json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_sanitize(v) for v in obj]
    if isinstance(obj, pd.Series):
        return _json_sanitize(obj.to_dict())
    if isinstance(obj, pd.DataFrame):
        return _json_sanitize(_df_to_json_records(obj))
    if isinstance(obj, datetime):
        return _to_iso_utc(obj)
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    return obj


def _json_safe_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    return _json_sanitize(records)


# ══════════════════════════════════════════════════════════════════════
# Schema helpers for the UI
# ══════════════════════════════════════════════════════════════════════

@app.get("/schema/events/fields")
async def schema_events_fields(
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    fields = [
        "ts", "source", "event_type", "user_id", "account_id",
        "device_id", "card_hash", "ip", "geo_lat", "geo_lon",
        "country", "city", "is_unusual",
        "office_id", "user_sign", "organization", "pnr", "carrier",
        "origin", "dest", "tkt_number", "status", "pos_country",
        "card_country", "advance_hours", "stay_nights", "amount",
        "currency", "fop_type", "fop_name", "fop_subtype",
        "card_last4", "card_bin", "is_fraud_indicator", "failure_reason", "legs",
    ]
    types = {
        "ts": "datetime", "source": "string", "event_type": "string",
        "user_id": "string", "account_id": "string", "device_id": "string",
        "card_hash": "string", "ip": "string", "geo_lat": "number",
        "geo_lon": "number", "country": "string", "city": "string",
        "is_unusual": "boolean",
        "office_id": "string", "user_sign": "string", "organization": "string",
        "pnr": "string", "carrier": "string", "origin": "string",
        "dest": "string", "tkt_number": "string", "status": "string",
        "pos_country": "string", "card_country": "string",
        "advance_hours": "number", "stay_nights": "number", "amount": "number",
        "currency": "string", "fop_type": "string", "fop_name": "string",
        "fop_subtype": "string", "card_last4": "string", "card_bin": "string",
        "is_fraud_indicator": "boolean", "failure_reason": "string", "legs": "string",
    }
    return {"fields": fields, "types": types}


@app.get("/schema/events/operators")
async def schema_events_operators() -> Dict[str, List[str]]:
    return {
        "string": ["eq", "ne", "in", "nin", "contains", "icontains", "startswith", "endswith"],
        "number": ["eq", "ne", "lt", "lte", "gt", "gte", "in", "nin"],
        "datetime": ["eq", "ne", "lt", "lte", "gt", "gte"],
        "boolean": ["eq", "ne"],
    }


@app.get("/schema/events/distinct")
async def schema_events_distinct(
    field: str = Query(..., description="Field name"),
    start: str = Query(...),
    end: str = Query(...),
    limit: int = Query(100, ge=1, le=10_000),
    session: AsyncSession = Depends(get_session),
):
    df = await _events_df(session, start, end, limit=200_000)
    if df.empty or field not in df.columns:
        return {"field": field, "values": []}
    vals = df[field].dropna().astype(str).value_counts().head(limit).index.tolist()
    return {"field": field, "values": vals}


# ══════════════════════════════════════════════════════════════════════
# Upload: preview & commit
# ══════════════════════════════════════════════════════════════════════

class UploadPreviewResponse(BaseModel):
    mapping: Dict[str, Union[str, Dict[str, Any]]]
    sample: List[Dict[str, Any]]


@app.post("/upload/preview", response_model=UploadPreviewResponse)
async def upload_preview(
    file: UploadFile = File(...),
    engine_name: str = Form("heuristic"),
    sample_rows: int = Form(25),
):
    content = await file.read()
    name = file.filename or "uploaded"
    try:
        if name.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(content))
        else:
            df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {e}")
    raw_mapping = suggest_event_mapping(df.head(50), engine=engine_name) or {}
    cols = set(df.columns)
    mapping = {
        k: v for k, v in raw_mapping.items()
        if v is not None and ((isinstance(v, str) and v in cols) or isinstance(v, dict))
    }
    sample = df.head(sample_rows).to_dict(orient="records")
    return UploadPreviewResponse(mapping=mapping, sample=sample)


class UploadCommitResponse(BaseModel):
    ingested: int
    source_name: str
    mapping_used: Dict[str, Optional[str]]
    mapping_exprs: Optional[Dict[str, str]] = None


# ── expression mapping helpers ───────────────────────────────────────

EXPR_TOKEN = re.compile(r"\$\{\s*([A-Za-z0-9_ .:\-]+)\s*\}")


def _eval_concat_expr(expr: str, row: Dict[str, Any]) -> str:
    if not isinstance(expr, str):
        return ""
    parts = [p.strip() for p in expr.split("+")]
    out_parts: List[str] = []
    for p in parts:
        def _repl(m: re.Match) -> str:
            col = m.group(1)
            v = row.get(col)
            if v is None and " " in col:
                v = row.get(col.strip())
            return "" if v is None else str(v)
        expanded = EXPR_TOKEN.sub(_repl, p)
        if len(expanded) >= 2 and (
            (expanded[0] == expanded[-1] == "'") or (expanded[0] == expanded[-1] == '"')
        ):
            expanded = expanded[1:-1]
        out_parts.append(expanded)
    return "".join(out_parts)


engine, async_session_maker = make_engine_and_session(DB_URL)


async def ingest_events_dataframe(df: pd.DataFrame, source_name: str) -> int:
    if "source" not in df.columns:
        df["source"] = source_name
    records = df.to_dict(orient="records")
    model_cols = set(c.name for c in Event.__table__.columns)
    clean_records = [{k: v for k, v in r.items() if k in model_cols} for r in records]
    async with async_session_maker() as session:
        await session.execute(Event.__table__.insert(), clean_records)
        await session.commit()
    return len(clean_records)


def evaluate_mapping_expression(expr: Dict[str, Any], frame: pd.DataFrame) -> pd.Series:
    op = (expr or {}).get("op")
    if op == "concat":
        cols: List[str] = expr.get("cols") or []
        sep: str = expr.get("sep") or ""
        parts = []
        for c in cols:
            s = frame.get(c)
            if s is None:
                s = pd.Series([""] * len(frame))
            parts.append(s.astype(str).fillna(""))
        return parts[0].str.cat(parts[1:], sep=sep) if parts else pd.Series([""] * len(frame))
    if op == "coalesce":
        cols = expr.get("cols") or []
        out = pd.Series([None] * len(frame))
        for c in cols:
            s = frame.get(c)
            if s is not None:
                out = out.fillna(s)
        return out.astype(str)
    if op == "lower":
        s = frame.get(expr.get("col"))
        return s.astype(str).str.lower() if s is not None else pd.Series([""] * len(frame))
    if op == "upper":
        s = frame.get(expr.get("col"))
        return s.astype(str).str.upper() if s is not None else pd.Series([""] * len(frame))
    if op == "try_float":
        s = frame.get(expr.get("col"))
        return pd.to_numeric(s, errors="coerce") if s is not None else pd.Series([float("nan")] * len(frame))
    if op == "parse_ts":
        s = frame.get(expr.get("col"))
        fmt = expr.get("fmt")
        return pd.to_datetime(s, format=fmt, errors="coerce", utc=False) if s is not None else pd.to_datetime([])
    raise ValueError(f"Unsupported expression op: {op}")


RECOGNIZED_TARGETS = {
    "ts", "event_type", "user_id", "account_id", "device_id", "card_hash",
    "ip", "geo_lat", "geo_lon", "country", "city", "amount", "is_unusual", "source", "meta",
}


@app.post("/upload/events", response_model=UploadCommitResponse)
async def upload_events(
    file: UploadFile = File(...),
    engine_name: str = Form("heuristic"),
    source_name: str = Form("uploaded_file"),
    mapping_json: str | None = Form(None),
    template_id: str | None = Form(None),
    validate: bool = Form(True),
):
    content = await file.read()
    name = file.filename or "uploaded"
    start_time = time.time()

    try:
        if name.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(content))
        elif name.endswith(".json"):
            try:
                df = pd.read_json(io.BytesIO(content), lines=True)
            except ValueError:
                df = pd.read_json(io.BytesIO(content))
        else:
            for encoding in ["utf-8", "latin-1", "cp1252"]:
                try:
                    df = pd.read_csv(io.BytesIO(content), encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError("Could not decode file with any supported encoding")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {e}")

    total_rows = len(df)
    template_name = None

    if mapping_json:
        try:
            mapping = json.loads(mapping_json)
            if not isinstance(mapping, dict):
                raise ValueError("mapping_json must be a JSON object")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid mapping_json: {e}")
    elif template_id:
        try:
            from app.mapping_api import _templates_store
            template = _templates_store.get(template_id)
            if template:
                mapping = template.get("mapping", {})
                template_name = template.get("name")
                exprs = template.get("expressions", {})
                if exprs:
                    mapping["__expr__"] = exprs
                template["use_count"] = template.get("use_count", 0) + 1
                template["last_used_at"] = datetime.utcnow().isoformat()
            else:
                raise HTTPException(status_code=404, detail=f"Template '{template_id}' not found")
        except ImportError:
            raise HTTPException(status_code=500, detail="Mapping API not available")
    else:
        mapping = suggest_event_mapping(df.head(50), engine=engine_name) or {}

    str_map = {k: v for k, v in mapping.items() if isinstance(v, str)}
    expr_map = {k: v for k, v in mapping.items() if isinstance(v, dict) and k != "__expr__"}
    inline_exprs = mapping.get("__expr__", {})

    cols = set(df.columns)
    unknown_pairs = {k: v for k, v in str_map.items() if v not in cols}
    if unknown_pairs:
        log.warning("dropping_unknown_sources", pairs=unknown_pairs)
        str_map = {k: v for k, v in str_map.items() if v in cols}

    required = {"ts", "event_type"}
    missing = {t for t in required if t not in str_map and t not in expr_map and t not in inline_exprs}
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required target(s): {', '.join(sorted(missing))}")

    work = df.copy()
    inverse = {src: tgt for tgt, src in str_map.items()}
    work = work.rename(columns=inverse)

    for tgt, expr in expr_map.items():
        try:
            work[tgt] = evaluate_mapping_expression(expr.get("__expr__", expr), work)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to compute expression for '{tgt}': {e}")

    for tgt, expr in inline_exprs.items():
        if tgt in work.columns:
            if isinstance(expr, str):
                if expr == "try_float":
                    work[tgt] = pd.to_numeric(work[tgt], errors="coerce")
                elif expr == "try_int":
                    work[tgt] = pd.to_numeric(work[tgt], errors="coerce").astype("Int64")
                elif expr == "upper":
                    work[tgt] = work[tgt].astype(str).str.upper()
                elif expr == "lower":
                    work[tgt] = work[tgt].astype(str).str.lower()
                elif expr == "trim":
                    work[tgt] = work[tgt].astype(str).str.strip()
                elif expr == "bool":
                    work[tgt] = work[tgt].apply(
                        lambda x: True if str(x).lower() in ("true", "1", "yes", "y", "on")
                        else False if str(x).lower() in ("false", "0", "no", "n", "off", "")
                        else None
                    )
            elif isinstance(expr, dict):
                op = expr.get("op")
                if op == "concat":
                    sep = expr.get("sep", "")
                    cols_to_concat = expr.get("cols", [])
                    if cols_to_concat:
                        work[tgt] = work[cols_to_concat].fillna("").astype(str).agg(sep.join, axis=1)
                elif op == "coalesce":
                    cols_to_coalesce = expr.get("cols", [])
                    default_val = expr.get("default")
                    if cols_to_coalesce:
                        work[tgt] = work[cols_to_coalesce].bfill(axis=1).iloc[:, 0]
                        if default_val is not None:
                            work[tgt] = work[tgt].fillna(default_val)

    if "ts" in work.columns:
        work["ts"] = pd.to_datetime(work["ts"], errors="coerce", utc=True)
        if work["ts"].isna().all():
            raise HTTPException(status_code=400, detail="All timestamps failed to parse for 'ts'")

    work["source"] = source_name

    validation_result = None
    rejected_count = 0
    if validate and template_id:
        try:
            from app.mapping_api import _templates_store
            from mapping.validation import DataValidator
            template = _templates_store.get(template_id)
            if template and template.get("validation_rules"):
                validator = DataValidator(template["validation_rules"])
                work, val_result = validator.validate(work)
                validation_result = val_result.to_dict()
                rejected_count = val_result.rejected_rows
                if val_result.rejected_rows > 0:
                    log.warning("validation_rejected_rows", count=val_result.rejected_rows, template=template_id)
        except ImportError:
            pass
        except Exception as val_err:
            log.warning("validation_failed", error=str(val_err))

    try:
        ingested = await ingest_events_dataframe(work, source_name=source_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingest failed: {e}")

    mapping_used = {**str_map, **expr_map}

    try:
        from app.mapping_api import _ingestion_logs
        log_id = str(uuid.uuid4())
        ingestion_log = {
            "id": log_id,
            "filename": file.filename or "uploaded",
            "source_name": source_name,
            "template_id": template_id,
            "template_name": template_name,
            "mapping_used": {**mapping_used, "__expr__": inline_exprs} if inline_exprs else mapping_used,
            "status": "completed",
            "rows_total": total_rows,
            "rows_ingested": int(ingested),
            "rows_rejected": rejected_count,
            "started_at": datetime.utcnow().isoformat(),
            "completed_at": datetime.utcnow().isoformat(),
            "duration_ms": int((time.time() - start_time) * 1000),
        }
        _ingestion_logs.append(ingestion_log)
        if len(_ingestion_logs) > 100:
            _ingestion_logs.pop(0)
        log.info("ingestion_logged", log_id=log_id, rows=int(ingested), source=source_name, template=template_name)
    except Exception as log_err:
        log.warning("failed_to_create_ingestion_log", error=str(log_err))

    return UploadCommitResponse(
        ingested=int(ingested),
        source_name=source_name,
        mapping_used=mapping_used,
        mapping_exprs=inline_exprs if inline_exprs else None,
    )


# ══════════════════════════════════════════════════════════════════════
# Batch upload (multi-file)
# ══════════════════════════════════════════════════════════════════════

class UploadFileResult(BaseModel):
    filename: str
    source_name: str
    ingested: int
    mapping_used: Dict[str, Optional[str]]
    mapping_exprs: Optional[Dict[str, str]] = None


class UploadBatchResponse(BaseModel):
    files: int
    total_ingested: int
    results: List[UploadFileResult]


@app.post("/upload/events/batch", response_model=UploadBatchResponse)
async def upload_events_batch(
    files: List[UploadFile] = File(...),
    engine_name: str = Form("heuristic"),
    source_prefix: str = Form("uploaded"),
    session: AsyncSession = Depends(get_session),
):
    results: List[UploadFileResult] = []
    total = 0

    for idx, file in enumerate(files, start=1):
        content = await file.read()
        name = file.filename or f"uploaded_{idx}"
        if name.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(content))
        else:
            df = pd.read_csv(io.BytesIO(content))

        mapping = suggest_event_mapping(df.head(50), engine=engine_name)
        source_name = f"{source_prefix}:{os.path.splitext(os.path.basename(name))[0]}"
        mapping["source_value"] = source_name

        event_rows: List[EventIn] = []
        for _, row in df.iterrows():
            src = row.to_dict()
            payload = {
                "ts": apply_mapping_with_expr(src, mapping, "ts"),
                "source": mapping.get("source_value", source_name),
                "event_type": apply_mapping_with_expr(src, mapping, "event_type", "event"),
                "user_id": apply_mapping_with_expr(src, mapping, "user_id"),
                "account_id": apply_mapping_with_expr(src, mapping, "account_id"),
                "device_id": apply_mapping_with_expr(src, mapping, "device_id"),
                "card_hash": str(apply_mapping_with_expr(src, mapping, "card_hash") or ""),
                "ip": apply_mapping_with_expr(src, mapping, "ip"),
                "geo_lat": None, "geo_lon": None,
                "country": apply_mapping_with_expr(src, mapping, "country"),
                "city": apply_mapping_with_expr(src, mapping, "city"),
                "office_id": apply_mapping_with_expr(src, mapping, "office_id"),
                "user_sign": apply_mapping_with_expr(src, mapping, "user_sign"),
                "organization": apply_mapping_with_expr(src, mapping, "organization"),
                "pnr": apply_mapping_with_expr(src, mapping, "pnr"),
                "carrier": apply_mapping_with_expr(src, mapping, "carrier"),
                "origin": apply_mapping_with_expr(src, mapping, "origin"),
                "dest": apply_mapping_with_expr(src, mapping, "dest"),
                "tkt_number": apply_mapping_with_expr(src, mapping, "tkt_number"),
                "status": apply_mapping_with_expr(src, mapping, "status"),
                "pos_country": apply_mapping_with_expr(src, mapping, "pos_country"),
                "card_country": apply_mapping_with_expr(src, mapping, "card_country"),
                "advance_hours": apply_mapping_with_expr(src, mapping, "advance_hours"),
                "stay_nights": apply_mapping_with_expr(src, mapping, "stay_nights"),
                "amount": apply_mapping_with_expr(src, mapping, "amount"),
                "currency": apply_mapping_with_expr(src, mapping, "currency"),
                "fop_type": apply_mapping_with_expr(src, mapping, "fop_type"),
                "fop_name": apply_mapping_with_expr(src, mapping, "fop_name"),
                "fop_subtype": apply_mapping_with_expr(src, mapping, "fop_subtype"),
                "card_last4": apply_mapping_with_expr(src, mapping, "card_last4"),
                "card_bin": apply_mapping_with_expr(src, mapping, "card_bin"),
                "is_fraud_indicator": apply_mapping_with_expr(src, mapping, "is_fraud_indicator"),
                "failure_reason": apply_mapping_with_expr(src, mapping, "failure_reason"),
                "legs": apply_mapping_with_expr(src, mapping, "legs"),
                "meta": {},
            }

            geo_lat = apply_mapping_with_expr(src, mapping, "geo_lat")
            geo_lon = apply_mapping_with_expr(src, mapping, "geo_lon")
            try:
                payload["geo_lat"] = float(geo_lat) if geo_lat not in [None, ""] and not (isinstance(geo_lat, float) and math.isnan(geo_lat)) else None
            except Exception:
                payload["geo_lat"] = None
            try:
                payload["geo_lon"] = float(geo_lon) if geo_lon not in [None, ""] and not (isinstance(geo_lon, float) and math.isnan(geo_lon)) else None
            except Exception:
                payload["geo_lon"] = None

            mapped_keys = set(mapping.values())
            meta = {}
            for k, v in src.items():
                if k in mapped_keys or k == "source":
                    continue
                meta[k] = v if v not in [None, ""] and not (isinstance(v, float) and math.isnan(v)) else None
            payload["meta"] = meta

            if not payload["ts"]:
                continue
            event_rows.append(EventIn(**payload))

        models = [Event.from_in(e) for e in event_rows]
        session.add_all(models)
        await session.commit()
        ingested_events.inc(len(models))

        exprs = mapping.get("__expr__") if isinstance(mapping.get("__expr__"), dict) else None
        mapping_clean = {k: v for k, v in mapping.items() if k != "__expr__"}
        results.append(UploadFileResult(
            filename=name, source_name=source_name, ingested=len(models),
            mapping_used=mapping_clean, mapping_exprs=exprs,
        ))
        total += len(models)

    return UploadBatchResponse(files=len(files), total_ingested=total, results=results)


# ══════════════════════════════════════════════════════════════════════
# Raw query endpoints
# ══════════════════════════════════════════════════════════════════════

@app.get("/query/raw")
async def query_raw(
    start: str, end: str,
    user: Optional[str] = None, account: Optional[str] = None,
    device: Optional[str] = None, ip: Optional[str] = None,
    source: Optional[str] = None, event_type: Optional[str] = None,
    country: Optional[str] = None, city: Optional[str] = None,
    is_unusual: Optional[bool] = None,
    limit: int = Query(10_000, ge=1, le=100_000),
    offset: int = Query(0, ge=0),
    sort_by: Optional[str] = None,
    sort_dir: str = Query("asc", pattern="^(asc|desc)$"),
    session: AsyncSession = Depends(get_session),
):
    df = await _events_df(session, start, end, limit=limit + offset)
    if df.empty:
        return JSONResponse(content=[])
    if user:
        df = df[df["user_id"] == user]
    if account:
        df = df[df["account_id"] == account]
    if device:
        df = df[df["device_id"] == device]
    if ip:
        df = df[df["ip"] == ip]
    if source:
        df = df[df["source"] == source]
    if event_type:
        df = df[df["event_type"] == event_type]
    if country:
        df = df[df["country"] == country]
    if city:
        df = df[df["city"] == city]
    if is_unusual is not None:
        df = df[df["is_unusual"].fillna(False) == bool(is_unusual)]
    if sort_by and sort_by in df.columns:
        df = df.sort_values(sort_by, ascending=(sort_dir == "asc"))
    df = df.iloc[offset: offset + limit]
    return JSONResponse(content=_json_safe_records(df))


# ── advanced query (JSON body) ───────────────────────────────────────

SUPPORTED_FIELDS = {
    "ts": "datetime", "source": "string", "event_type": "string",
    "user_id": "string", "account_id": "string", "device_id": "string",
    "card_hash": "string", "ip": "string", "geo_lat": "number",
    "geo_lon": "number", "country": "string", "city": "string",
    "is_unusual": "boolean",
}


class FilterCond(BaseModel):
    field: str
    op: str
    value: Any


class AdvancedQuery(BaseModel):
    start: str
    end: str
    where: List[FilterCond] = Field(default_factory=list)
    select: Optional[List[str]] = None
    sort_by: Optional[str] = None
    sort_dir: str = "asc"
    limit: int = 10_000
    offset: int = 0


def _get_series_for_field(df: pd.DataFrame, field: str) -> pd.Series | None:
    if field in df.columns:
        return df[field]
    if field.startswith("meta.") and "meta" in df.columns:
        key = field.split(".", 1)[1]
        return df["meta"].apply(lambda m: m.get(key) if isinstance(m, dict) else None)
    return None


def _normalize_cond(cond: Any) -> Dict[str, Any] | None:
    if hasattr(cond, "field") and hasattr(cond, "op"):
        return {"field": getattr(cond, "field"), "op": getattr(cond, "op"), "value": getattr(cond, "value", None)}
    if isinstance(cond, dict):
        f, op = cond.get("field"), cond.get("op")
        if f and op:
            return {"field": f, "op": op, "value": cond.get("value")}
    return None


def _apply_where(df: pd.DataFrame, where: Iterable[Any]) -> pd.DataFrame:
    if not where:
        return df
    out = df
    for raw in where:
        c = _normalize_cond(raw)
        if not c:
            continue
        field, op, value = c["field"], c["op"], c.get("value")
        s = _get_series_for_field(out, field)
        if s is None:
            continue
        is_ts = field == "ts"
        if is_ts:
            s = pd.to_datetime(s, utc=True, errors="coerce")
            value_dt = pd.to_datetime(value, utc=True, errors="coerce") if value is not None else value
        try:
            if op == "eq":
                mask = (s == (value_dt if is_ts else value))
            elif op == "ne":
                mask = (s != (value_dt if is_ts else value))
            elif op == "lt":
                mask = (s < (value_dt if is_ts else value))
            elif op == "lte":
                mask = (s <= (value_dt if is_ts else value))
            elif op == "gt":
                mask = (s > (value_dt if is_ts else value))
            elif op == "gte":
                mask = (s >= (value_dt if is_ts else value))
            elif op == "in":
                mask = s.isin(value if isinstance(value, (list, tuple, set)) else [value])
            elif op == "nin":
                mask = ~s.isin(value if isinstance(value, (list, tuple, set)) else [value])
            elif op == "contains":
                mask = s.astype(str).str.contains(str(value), case=True, na=False)
            elif op == "icontains":
                mask = s.astype(str).str.contains(str(value), case=False, na=False)
            elif op == "startswith":
                mask = s.astype(str).str.startswith(str(value), na=False)
            elif op == "endswith":
                mask = s.astype(str).str.endswith(str(value), na=False)
            else:
                continue
        except Exception:
            continue
        out = out[mask]
    return out


@app.post("/query/raw/advanced")
async def query_raw_advanced(
    q: AdvancedQuery = Body(...),
    session: AsyncSession = Depends(get_session),
):
    df = await _events_df(session, q.start, q.end, limit=q.limit + q.offset)
    if df.empty:
        return JSONResponse(content=[])
    df = _apply_where(df, q.where)
    if q.select:
        keep = [c for c in q.select if c in df.columns]
        if keep:
            df = df[keep]
    if q.sort_by and q.sort_by in df.columns:
        df = df.sort_values(q.sort_by, ascending=(q.sort_dir == "asc"))
    df = df.iloc[q.offset: q.offset + q.limit]
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return JSONResponse(content=_json_safe_records(df))


# ══════════════════════════════════════════════════════════════════════
# Analytics endpoints (engine-aware)
# ══════════════════════════════════════════════════════════════════════

@app.get("/analytics/advanced")
async def analytics_advanced(
    start: str, end: str,
    contamination: float = 0.05, speed_thr: float = 900.0,
    dist_thr: float = 2000.0, cluster: bool = False,
    limit: int = 50_000,
    session: AsyncSession = Depends(get_session),
):
    from app.viz_engine import get_viz_data
    df = await get_viz_data(
        start=start, end=end, limit=limit, analytics="advanced",
        contamination=contamination, speed_thr=speed_thr, dist_thr=dist_thr,
        session=session,
    )
    if df.empty:
        return JSONResponse(content=[])
    if cluster:
        df = cluster_geo_temporal(df)
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return JSONResponse(content=_json_safe_records(df))


class AdvancedRunBody(BaseModel):
    start: str
    end: str
    contamination: float = 0.05
    speed_thr: float = 900.0
    dist_thr: float = 2000.0
    cluster: bool = False
    where: List[FilterCond] = Field(default_factory=list)


@app.post("/analytics/advanced")
async def analytics_advanced_post(
    body: AdvancedRunBody = Body(...),
    session: AsyncSession = Depends(get_session),
):
    from app.viz_engine import get_viz_data
    df = await get_viz_data(
        start=body.start, end=body.end, limit=100_000,
        where=body.where if body.where else None,
        analytics="advanced", contamination=body.contamination,
        speed_thr=body.speed_thr, dist_thr=body.dist_thr, session=session,
    )
    if df.empty:
        return JSONResponse(content=[])
    if body.cluster:
        df = cluster_geo_temporal(df)
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return JSONResponse(content=_json_safe_records(df))


# ══════════════════════════════════════════════════════════════════════
# Visualization APIs (engine-aware)
# ══════════════════════════════════════════════════════════════════════

AnalyticsMode = Literal["none", "simple", "advanced"]
Metric = Literal["count", "avg", "max", "sum"]
_BUCKET_TO_FREQ = {
    "1m": "min", "5m": "5min", "15m": "15min", "1h": "h", "6h": "6h", "1d": "1D",
}


def _is_numeric_series(s: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(s)


class VizCommonParams(BaseModel):
    start: str
    end: str
    analytics: AnalyticsMode = "none"
    contamination: float = 0.05
    speed_thr: float = 900.0
    dist_thr: float = 2000.0
    where: List[FilterCond] = Field(default_factory=list)
    tz: Optional[str] = None
    source: Optional[str] = None


# ── Timeline ─────────────────────────────────────────────────────────

class TimelineQuery(VizCommonParams):
    top_n: int = 8
    z_thr: float = 3.0
    bucket: Literal["30s", "1m", "2m", "5m", "10m", "15m", "30m", "1h", "3h", "6h", "12h", "1d"] = "5m"
    metric: Metric = "count"
    value_field: Optional[str] = None
    group_field: Optional[str] = "user_id"
    normalize: bool = False
    smooth: int = 0
    source: Optional[str] = None


@app.get("/viz/timeline")
async def viz_timeline_get(
    start: str, end: str, top_n: int = 8,
    analytics: AnalyticsMode = "none", z_thr: float = 3.0,
    contamination: float = 0.05, speed_thr: float = 900.0, dist_thr: float = 2000.0,
    session: AsyncSession = Depends(get_session),
):
    body = TimelineQuery(
        start=start, end=end, top_n=top_n, analytics=analytics,
        z_thr=z_thr, contamination=contamination, speed_thr=speed_thr, dist_thr=dist_thr,
    )
    return await _timeline_impl(body, session)


@app.post("/viz/timeline")
async def viz_timeline_post(body: TimelineQuery = Body(...), session: AsyncSession = Depends(get_session)):
    return await _timeline_impl(body, session)


async def _timeline_impl(body: TimelineQuery, session: AsyncSession):
    from app.viz_engine import get_viz_data

    df = await get_viz_data(
        start=body.start, end=body.end, limit=200_000, source=body.source,
        where=body.where if body.where else None, analytics=body.analytics,
        z_thr=body.z_thr, contamination=body.contamination,
        speed_thr=body.speed_thr, dist_thr=body.dist_thr,
        bucket=body.bucket, session=session,
    )

    empty_payload = {
        "minutes": [], "series": [], "anom_by_minute": {},
        "anom_by_minute_score": {}, "reasons_by_minute": {},
        "explain_by_minute": {}, "anomaly_events": {}, "thresholds": {},
        "top_users": [], "rows": [], "metric": body.metric,
        "value_field": body.value_field, "group_field": body.group_field or "user_id",
        "groups": [], "normalize": body.normalize, "smooth": body.smooth,
    }

    if df.empty:
        return JSONResponse(content=_json_sanitize(empty_payload))

    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    dwork = df.copy()

    # Bucketing
    freq_map = {
        "30s": "30s", "1m": "min", "2m": "2min", "5m": "5min",
        "10m": "10min", "15m": "15min", "30m": "30min",
        "1h": "h", "3h": "3h", "6h": "6h", "12h": "12h", "1d": "1D",
    }
    freq = freq_map.get(body.bucket or "5m", "5min")
    dwork["bucket"] = dwork["ts"].dt.floor(freq)

    # Dynamic group column(s)
    group_fields_raw = body.group_field or "user_id"
    group_fields = [f.strip() for f in group_fields_raw.split(",") if f.strip()]
    valid_group_fields = []
    for gf in group_fields:
        if gf in dwork.columns:
            valid_group_fields.append(gf)
        elif gf.startswith("meta.") and "meta" in dwork.columns:
            meta_key = gf.split(".", 1)[1]
            dwork[gf] = dwork["meta"].apply(lambda m: m.get(meta_key) if isinstance(m, dict) else None)
            valid_group_fields.append(gf)
    if not valid_group_fields:
        valid_group_fields = ["user_id"] if "user_id" in dwork.columns else ["event_type"]
    if len(valid_group_fields) > 1:
        dwork["__group_key"] = dwork[valid_group_fields].astype(str).agg(" | ".join, axis=1)
        group_col = "__group_key"
    else:
        group_col = valid_group_fields[0]

    # Aggregation
    metric = (body.metric or "count").lower()
    val = body.value_field
    if metric == "count" or not val or val not in dwork.columns or not _is_numeric_series(dwork[val]):
        pivot = dwork.groupby(["bucket", group_col]).size().unstack(fill_value=0)
    else:
        agg = {"avg": "mean", "max": "max", "sum": "sum", "min": "min"}.get(metric, "mean")
        pivot = dwork.groupby(["bucket", group_col])[val].agg(agg).unstack(fill_value=0)

    totals = pivot.sum(axis=0).sort_values(ascending=False)
    top_groups = [g for g in totals.head(body.top_n).index if pd.notna(g)]
    minutes_idx = pivot.index.sort_values()
    top_df = pivot[top_groups] if top_groups else pivot.iloc[:, :0]
    others = pivot.drop(columns=top_groups, errors="ignore").sum(axis=1) if len(pivot.columns) else pd.Series(0, index=minutes_idx)

    if body.normalize:
        denom = top_df.sum(axis=1) + others
        denom = denom.replace(0, pd.NA).fillna(1)
        if len(top_df.columns):
            top_df = top_df.div(denom, axis=0).fillna(0.0) * 100
        others = (others / denom).fillna(0.0) * 100

    if isinstance(body.smooth, int) and body.smooth >= 2:
        win = int(body.smooth)
        if len(top_df.columns):
            top_df = top_df.rolling(win, min_periods=1).mean()
        others = others.rolling(win, min_periods=1).mean()

    minutes = pd.Index(minutes_idx).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ").tolist()

    def _values_for(series: pd.Series) -> list:
        return [float(series.get(m, 0.0)) for m in minutes_idx]

    series = [{"name": str(g), "values": _values_for(top_df[g])} for g in top_groups]
    series.append({"name": "Others", "values": [float(others.get(m, 0.0)) for m in minutes_idx]})

    rows = _json_safe_records(
        dwork.assign(
            ts=dwork["ts"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            bucket=dwork["bucket"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
    )

    # Anomaly overlays
    anom_by_minute: Dict[str, float] = {}
    anom_by_minute_score: Dict[str, float] = {}
    reasons_by_minute: Dict[str, List[str]] = {}
    explain_by_minute: Dict[str, str] = {}
    anomaly_events: Dict[str, List[Dict]] = {}
    thresholds: Dict[str, Any] = {}

    if body.analytics in ("simple", "advanced"):
        df_ts = pd.to_datetime(dwork["ts"], utc=True, errors="coerce")
        dwork = dwork.assign(__bucket=df_ts.dt.floor(freq))

        if body.analytics == "simple":
            if "zrobust" in dwork.columns:
                dwork["anom_score"] = 1.0 / (1.0 + np.exp(-dwork["zrobust"].fillna(0)))
            elif "zscore" in dwork.columns:
                dwork["anom_score"] = 1.0 / (1.0 + np.exp(-dwork["zscore"].fillna(0)))
            z_thr_val = float(body.z_thr)
            score_threshold = 1.0 / (1.0 + np.exp(-z_thr_val))
            if "anom_score" not in dwork.columns:
                dwork["anom_score"] = 0.0
            if "anomaly" not in dwork.columns:
                dwork["anomaly"] = dwork["anom_score"] >= score_threshold

        if "anomaly" in dwork.columns:
            dd = dwork[dwork["anomaly"].fillna(False)].copy()
        else:
            dd = pd.DataFrame()

        if not dd.empty:
            key = dd["__bucket"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            if "anom_score" in dd.columns:
                s = dd.groupby(key)["anom_score"].max()
                anom_by_minute_score = {k: float(v) for k, v in s.items() if pd.notna(v)}
            if "zrobust" in dd.columns:
                z = dd.groupby(key)["zrobust"].max()
                anom_by_minute = {k: float(v) for k, v in z.items() if pd.notna(v)}
            elif "zscore" in dd.columns:
                z = dd.groupby(key)["zscore"].max()
                anom_by_minute = {k: float(v) for k, v in z.items() if pd.notna(v)}
            if "reasons" in dd.columns:
                def _codes_each(rs):
                    try:
                        return [r.get("code", "") for r in (rs or []) if isinstance(r, dict)]
                    except Exception:
                        return []
                codes = dd.assign(__codes=dd["reasons"].apply(_codes_each))
                top = codes.groupby(key)["__codes"].apply(
                    lambda s: pd.Series([c for lst in s for c in (lst or [])]).value_counts().head(3).index.tolist() if len(s) else []
                )
                reasons_by_minute = {k: v for k, v in top.items()}
            if "explain" in dd.columns:
                exp = dd.assign(__k=key).groupby("__k")["explain"].apply(
                    lambda s: "; ".join(sorted(set([e for e in s if isinstance(e, str) and e]))[:3])
                )
                explain_by_minute = {k: v for k, v in exp.items()}
            for bucket_key in anom_by_minute_score.keys():
                bucket_events = dd[dd["__bucket"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ") == bucket_key]
                events_list = []
                for _, row in bucket_events.head(20).iterrows():
                    event_dict = {
                        "ts": row.get("ts").isoformat() if pd.notna(row.get("ts")) else None,
                        "user_id": row.get("user_id"), "event_type": row.get("event_type"),
                        "anom_score": float(row.get("anom_score", 0)),
                        "reasons": row.get("reasons", []), "explain": row.get("explain", ""),
                    }
                    for col in ["ip", "device_id", "country", "city", "account_id"]:
                        if col in row.index and pd.notna(row[col]):
                            event_dict[col] = row[col]
                    events_list.append(event_dict)
                anomaly_events[bucket_key] = events_list

        if body.analytics == "simple":
            z_thr_val = float(body.z_thr)
            score_thr = 1.0 / (1.0 + np.exp(-z_thr_val))
            thresholds = {"mode": "simple", "z_thr": z_thr_val, "score_thr": float(score_thr)}
        else:
            contamination = float(body.contamination)
            if "anom_score" in dwork.columns and not dwork["anom_score"].empty:
                q = float(dwork["anom_score"].quantile(1.0 - contamination))
            else:
                q = 0.95
            thresholds = {"mode": "advanced", "contamination": contamination, "score_quantile": q}

    try:
        minutes = pd.to_datetime(minutes, utc=True).strftime("%Y-%m-%dT%H:%M:%SZ").tolist()
    except Exception:
        minutes = [str(m) for m in minutes]

    payload = {
        "minutes": minutes, "series": series,
        "anom_by_minute": anom_by_minute, "anom_by_minute_score": anom_by_minute_score,
        "reasons_by_minute": reasons_by_minute, "explain_by_minute": explain_by_minute,
        "anomaly_events": anomaly_events, "thresholds": thresholds,
        "top_users": top_groups, "rows": rows,
        "metric": body.metric, "value_field": body.value_field,
        "group_field": group_col, "groups": top_groups,
        "normalize": body.normalize, "smooth": body.smooth,
    }
    return JSONResponse(content=_json_sanitize(payload))


# ── Globe ────────────────────────────────────────────────────────────

@app.get("/viz/globe")
async def viz_globe_get(
    start: str, end: str,
    analytics: AnalyticsMode = "none", contamination: float = 0.05,
    speed_thr: float = 900.0, dist_thr: float = 2000.0,
    cluster: bool = False, bucket: Optional[str] = None,
    limit: int = 50_000, session: AsyncSession = Depends(get_session),
):
    body = GlobeQuery(
        start=start, end=end, analytics=analytics,
        contamination=contamination, speed_thr=speed_thr, dist_thr=dist_thr,
        cluster=cluster, bucket=bucket,
    )
    return await _globe_impl(body, limit, session)


@app.post("/viz/globe")
async def viz_globe_post(body: GlobeQuery = Body(...), limit: int = 50_000, session: AsyncSession = Depends(get_session)):
    return await _globe_impl(body, limit, session)


_BUCKET_TO_FREQ_FULL = {
    "30s": "30s", "1m": "min", "2m": "2min", "5m": "5min", "10m": "10min",
    "15m": "15min", "30m": "30min", "1h": "h", "3h": "3h", "6h": "6h",
    "12h": "12h", "1d": "1D",
}


def _pull_meta(df: pd.DataFrame, key: str, into: str):
    if key not in df.columns and "meta" in df.columns:
        df[into] = df["meta"].apply(lambda x: x.get(key) if isinstance(x, dict) else None)


def _safe_float(s):
    return pd.to_numeric(s, errors="coerce")


async def _globe_impl(body: GlobeQuery, limit: int, session: AsyncSession):
    from app.viz_engine import get_viz_data

    df = await get_viz_data(
        start=body.start, end=body.end, limit=limit,
        source=body.source, where=body.where if body.where else None,
        analytics=body.analytics, z_thr=getattr(body, "z_thr", 3.0),
        contamination=body.contamination, speed_thr=body.speed_thr,
        dist_thr=body.dist_thr, bucket=getattr(body, "bucket", None),
        session=session,
    )

    if df.empty:
        return JSONResponse(content={"events": [], "routes": [], "thresholds": {}})

    _pull_meta(df, "geo_lat", "geo_lat")
    _pull_meta(df, "geo_lon", "geo_lon")
    _pull_meta(df, "origin", "origin")
    _pull_meta(df, "dest", "dest")
    _pull_meta(df, "carrier", "carrier")

    if body.cluster:
        df = cluster_geo_temporal(df)

    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    if body.bucket:
        freq = _BUCKET_TO_FREQ_FULL.get(body.bucket, "T")
        if body.tz:
            try:
                df["ts"] = df["ts"].dt.tz_convert(body.tz)
            except Exception:
                pass
        df["ts"] = df["ts"].dt.floor(freq)
        df["ts"] = df["ts"].dt.tz_convert("UTC")
        df["ts"] = df["ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    routes_out = []
    if body.route_mode:
        rdf = df.copy()
        if body.carrier:
            rdf = rdf[rdf["carrier"] == body.carrier]
        rdf = rdf.dropna(subset=["origin", "dest"])
        val_field = None
        if body.analytics != "none":
            val_field = body.value_field or ("anom_score" if "anom_score" in rdf.columns else None)
        g = rdf.groupby(["origin", "dest"])
        if val_field is None or body.route_metric == "count":
            agg = g.size().reset_index(name="value")
        elif body.route_metric == "max":
            agg = g[val_field].max().reset_index(name="value")
        elif body.route_metric == "sum":
            agg = g[val_field].sum().reset_index(name="value")
        elif body.route_metric == "avg":
            agg = g[val_field].mean().reset_index(name="value")
        else:
            agg = g.size().reset_index(name="value")
        cnt = g.size().reset_index(name="count")
        agg = agg.merge(cnt, on=["origin", "dest"], how="left")
        routes_out = [
            {"o": str(r["origin"]), "d": str(r["dest"]), "count": int(r["count"]), "value": float(r["value"])}
            for _, r in agg.iterrows()
        ]

    thresholds = {}
    if body.analytics == "advanced":
        try:
            q = float(df["anom_score"].quantile(1.0 - float(body.contamination))) if "anom_score" in df.columns and not df.empty else 0.95
        except Exception:
            q = 0.95
        thresholds = {"mode": "advanced", "contamination": float(body.contamination), "score_quantile": q}

    payload = {"events": _json_safe_records(df), "routes": routes_out, "thresholds": thresholds}
    return JSONResponse(content=payload)


# ── Grid ─────────────────────────────────────────────────────────────

class GridQuery(VizCommonParams):
    sort_by: Optional[str] = None
    sort_dir: Literal["asc", "desc"] = "asc"
    limit: int = 50_000
    offset: int = 0
    select: Optional[List[str]] = None
    bucket: Optional[Literal["1m", "5m", "15m", "1h", "6h", "1d"]] = None
    aggregate: bool = False
    group_by: List[str] = Field(default_factory=list)
    metric: Metric = "count"
    value_field: Optional[str] = None
    z_thr: float = 3.0
    source: Optional[str] = None


@app.get("/viz/grid")
async def viz_grid_get(
    start: str, end: str,
    analytics: AnalyticsMode = "none", contamination: float = 0.05,
    speed_thr: float = 900.0, dist_thr: float = 2000.0,
    sort_by: Optional[str] = None, sort_dir: Literal["asc", "desc"] = "asc",
    bucket: Optional[str] = None,
    limit: int = Query(50_000, ge=1, le=100_000),
    offset: int = Query(0, ge=0),
    source: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
    z_thr: float = 3.0,
):
    body = GridQuery(
        start=start, end=end, analytics=analytics, contamination=contamination,
        speed_thr=speed_thr, dist_thr=dist_thr, sort_by=sort_by, sort_dir=sort_dir,
        limit=limit, offset=offset, bucket=bucket, z_thr=z_thr, source=source,
    )
    return await _grid_impl(body, session)


@app.post("/viz/grid")
async def viz_grid_post(body: GridQuery = Body(...), session: AsyncSession = Depends(get_session)):
    return await _grid_impl(body, session)


async def _grid_impl(body: GridQuery, session: AsyncSession):
    from app.viz_engine import get_viz_data

    df = await get_viz_data(
        start=body.start, end=body.end, limit=body.limit + body.offset,
        source=body.source, where=body.where if body.where else None,
        analytics=body.analytics, z_thr=body.z_thr,
        contamination=body.contamination, speed_thr=body.speed_thr,
        dist_thr=body.dist_thr, bucket=body.bucket, session=session,
    )

    if df.empty:
        return JSONResponse(content=[])

    # Aggregate path
    if body.aggregate:
        if "bucket" in (body.group_by or []) or body.bucket:
            freq = _BUCKET_TO_FREQ.get(body.bucket or "1m", "T")
            df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
            df["bucket"] = df["ts"].dt.floor(freq).dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        groupers = [g for g in (body.group_by or []) if g in df.columns or g == "bucket"]
        if not groupers:
            groupers = ["bucket"] if "bucket" in (body.group_by or []) or body.bucket else []

        metric = (body.metric or "count").lower()
        val = body.value_field
        if metric == "count" or (not val) or (val not in df.columns) or (not _is_numeric_series(df[val])):
            g = df.groupby(groupers).size().reset_index(name="value")
        else:
            agg_func = {"avg": "mean", "max": "max", "sum": "sum"}.get(metric, "mean")
            g = df.groupby(groupers)[val].agg(agg_func).reset_index(name="value")
        out = g

        if "anom_score" in df.columns:
            g_anom = df.groupby(groupers)["anom_score"].max().reset_index(name="anom_score")
            out = out.merge(g_anom, on=groupers, how="left")
            try:
                thr = float(out["anom_score"].fillna(0).quantile(1 - getattr(body, "contamination", 0.05)))
            except Exception:
                thr = 0.0
            out["anomaly"] = out["anom_score"].fillna(0) >= thr

        if "reasons" in df.columns:
            tmp = df.copy()
            tmp["_codes"] = tmp["reasons"].apply(lambda rs: [r.get("code") for r in (rs or [])] if isinstance(rs, list) else [])
            tmp = tmp.explode("_codes")
            rx = tmp.groupby(groupers)["_codes"].apply(lambda s: s.dropna().value_counts().head(1).index.tolist()).reset_index()
            rx["_codes"] = rx["_codes"].apply(lambda lst: lst[0] if lst else None)
            rx = rx.rename(columns={"_codes": "top_reason"})
            out = out.merge(rx, on=groupers, how="left")

        if body.select:
            keep = [c for c in body.select if c in out.columns]
            if keep:
                out = out[keep]
        if body.sort_by and body.sort_by in out.columns:
            out = out.sort_values(body.sort_by, ascending=(body.sort_dir == "asc"))
        out = out.iloc[body.offset: body.offset + body.limit]
        return JSONResponse(content=_json_safe_records(out))

    # Raw rows path
    if body.bucket:
        freq = _BUCKET_TO_FREQ.get(body.bucket, "T")
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        if body.tz:
            try:
                df["ts"] = df["ts"].dt.tz_convert(body.tz)
            except Exception:
                pass
        df["ts"] = df["ts"].dt.floor(freq)
        df["ts"] = df["ts"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    if body.select:
        keep = [c for c in body.select if c in df.columns]
        if keep:
            df = df[keep]
    if body.sort_by and body.sort_by in df.columns:
        df = df.sort_values(body.sort_by, ascending=(body.sort_dir == "asc"))
    df = df.iloc[body.offset: body.offset + body.limit]
    return JSONResponse(content=_json_safe_records(df))


# ── Graph ────────────────────────────────────────────────────────────

class GraphQuery(VizCommonParams):
    edges: List[str] = Field(default_factory=lambda: [
        "user_id-account_id", "user_id-device_id", "user_id-ip",
        "user_id-card_hash", "device_id-ip",
    ])
    metric: Metric = "count"
    value_field: Optional[str] = None
    min_link_value: float = 1.0
    max_nodes: int = 4000
    max_links: int = 6000
    z_thr: float = 3.0


@app.post("/viz/graph")
async def viz_graph_post(body: GraphQuery = Body(...), session: AsyncSession = Depends(get_session)):
    return await _graph_impl(body, session)


@app.get("/viz/graph")
async def viz_graph_get(
    start: str, end: str,
    analytics: AnalyticsMode = "none", contamination: float = 0.05,
    speed_thr: float = 900.0, dist_thr: float = 2000.0,
    edges: Optional[str] = None, metric: Metric = "count",
    value_field: Optional[str] = None, min_link_value: float = 1.0,
    max_nodes: int = 4000, max_links: int = 6000,
    session: AsyncSession = Depends(get_session),
):
    body = GraphQuery(
        start=start, end=end, analytics=analytics,
        contamination=contamination, speed_thr=speed_thr, dist_thr=dist_thr,
        edges=(edges.split(",") if edges else None) or GraphQuery().edges,
        metric=metric, value_field=value_field,
        min_link_value=min_link_value, max_nodes=max_nodes, max_links=max_links,
        where=[], z_thr=3.0,
    )
    return await _graph_impl(body, session)


def _compute_communities(nodes: list[dict], links: list[dict]) -> dict[str, int]:
    if not nodes or not links:
        return {}
    if HAVE_NX:
        G = nx.Graph()
        for n in nodes:
            G.add_node(n["id"])
        for e in links:
            a, b = e.get("source"), e.get("target")
            if a is None or b is None or a == b:
                continue
            w = float(e.get("value", 1.0))
            if G.has_edge(a, b):
                G[a][b]["weight"] = G[a][b].get("weight", 1.0) + w
            else:
                G.add_edge(a, b, weight=w)
        comms = None
        try:
            comms = list(greedy_modularity_communities(G, weight="weight"))
        except Exception:
            try:
                comms = list(asyn_lpa_communities(G, weight="weight"))
            except Exception:
                comms = [set(G.nodes())]
        id_to_comm: dict[str, int] = {}
        for idx, group in enumerate(sorted(comms, key=lambda s: -len(s))):
            for nid in group:
                id_to_comm[str(nid)] = idx
        return id_to_comm

    from collections import Counter, defaultdict
    neigh = defaultdict(set)
    for e in links:
        a, b = str(e.get("source")), str(e.get("target"))
        if a and b and a != b:
            neigh[a].add(b)
            neigh[b].add(a)
    labels = {str(n["id"]): i for i, n in enumerate(nodes)}
    for _ in range(3):
        changed = False
        for v in list(labels.keys()):
            cnt = Counter(labels.get(u) for u in neigh.get(v, []))
            if not cnt:
                continue
            new = cnt.most_common(1)[0][0]
            if new is not None and new != labels[v]:
                labels[v] = new
                changed = True
        if not changed:
            break
    label_map, next_id = {}, 0
    out: dict[str, int] = {}
    for nid, lab in labels.items():
        if lab not in label_map:
            label_map[lab] = next_id
            next_id += 1
        out[nid] = label_map[lab]
    return out


async def _graph_impl(body: GraphQuery, session: AsyncSession):
    from app.viz_engine import get_viz_data

    df = await get_viz_data(
        start=body.start, end=body.end, limit=300_000,
        source=getattr(body, "source", None),
        where=body.where if body.where else None,
        analytics=body.analytics, z_thr=body.z_thr,
        contamination=body.contamination, speed_thr=body.speed_thr,
        dist_thr=body.dist_thr, session=session,
    )

    if df.empty:
        return JSONResponse(content={"nodes": [], "links": []})

    candidate_cols = set()
    for e in body.edges:
        a, b = e.split("-", 1)
        candidate_cols.add(a)
        candidate_cols.add(b)
    present = [c for c in candidate_cols if c in df.columns]
    if not present:
        return JSONResponse(content={"nodes": [], "links": []})

    links_all: List[Dict[str, Any]] = []
    metric = (body.metric or "count").lower()
    val = body.value_field
    use_value = metric != "count" and val and (val in df.columns) and _is_numeric_series(df[val])
    for e in body.edges:
        a, b = e.split("-", 1)
        if a not in df.columns or b not in df.columns:
            continue
        sub = df[[a, b] + ([val] if use_value else [])].dropna(subset=[a, b])
        if sub.empty:
            continue
        if use_value:
            agg_func = {"avg": "mean", "max": "max", "sum": "sum"}.get(metric, "mean")
            gb = sub.groupby([a, b])[val].agg(agg_func).reset_index(name="value")
        else:
            gb = sub.groupby([a, b]).size().reset_index(name="value")
        gb["etype"] = f"{a}-{b}"
        gb = gb[gb["value"] >= body.min_link_value]
        gb = gb.sort_values("value", ascending=False).head(body.max_links)
        links_all.extend(_json_safe_records(gb))

    if not links_all:
        return JSONResponse(content={"nodes": [], "links": []})

    # Anomaly glow per entity
    entity_to_anom: Dict[tuple, tuple] = {}

    def _update_max(key: tuple, score: float, reasons):
        cur = entity_to_anom.get(key, (0.0, []))
        if score > cur[0]:
            entity_to_anom[key] = (float(score), reasons or [])

    if "anom_score" in df.columns:
        for col in ["user_id", "account_id", "device_id", "ip", "card_hash"]:
            if col in df.columns:
                rs_series = df["reasons"] if "reasons" in df.columns else [[] for _ in range(len(df))]
                for v, score, rs in zip(df[col].astype(str), df["anom_score"], rs_series):
                    if pd.isna(v):
                        continue
                    _update_max((col, v), float(score), rs if isinstance(rs, list) else [])

    from collections import defaultdict
    node_value = defaultdict(float)
    node_type = {}

    def nid(t: str, label: Any) -> str:
        return f"{t}:{label}"

    for lk in links_all:
        a, b = lk.get("etype", "").split("-", 1)
        ida, idb = nid(a, lk[a]), nid(b, lk[b])
        w = float(lk["value"])
        node_value[ida] += w
        node_value[idb] += w
        node_type[ida] = a
        node_type[idb] = b

    nodes_sorted = sorted(node_value.items(), key=lambda kv: kv[1], reverse=True)[:body.max_nodes]
    keep_nodes = set(n for n, _ in nodes_sorted)

    filtered_links = []
    for lk in links_all:
        a, b = lk.get("etype", "").split("-", 1)
        ida, idb = nid(a, lk[a]), nid(b, lk[b])
        if ida in keep_nodes and idb in keep_nodes:
            filtered_links.append({"source": ida, "target": idb, "etype": lk["etype"], "value": float(lk["value"])})

    nodes = [
        {
            "id": n, "type": node_type[n], "label": n.split(":", 1)[1],
            "value": float(v),
            "anom_max": float(entity_to_anom.get((node_type[n], n.split(":", 1)[1]), (0.0, []))[0]),
            "reasons_top": [r.get("code", "") for r in entity_to_anom.get((node_type[n], n.split(":", 1)[1]), (0.0, []))[1]][:3],
        }
        for n, v in nodes_sorted
    ]

    deg = {}
    for lk in filtered_links:
        deg[lk["source"]] = deg.get(lk["source"], 0) + 1
        deg[lk["target"]] = deg.get(lk["target"], 0) + 1
    for n in nodes:
        n["degree"] = int(deg.get(n["id"], 0))

    id_to_comm = _compute_communities(nodes, filtered_links)
    for n in nodes:
        n["community"] = int(id_to_comm.get(n["id"], 0))

    return JSONResponse(content={"nodes": nodes, "links": filtered_links})


# ══════════════════════════════════════════════════════════════════════
# Top users endpoint
# ══════════════════════════════════════════════════════════════════════

class TopUsersQuery(BaseModel):
    start: str
    end: str
    n: int = 50
    group_field: str = "user_id"
    where: List[FilterCond] = Field(default_factory=list)


@app.get("/analytics/top-users")
async def analytics_top_users_get(
    start: str, end: str, n: int = 50, group_field: str = "user_id",
    session: AsyncSession = Depends(get_session),
):
    return await _top_users_impl(TopUsersQuery(start=start, end=end, n=n, group_field=group_field), session)


@app.post("/analytics/top-users")
async def analytics_top_users_post(body: TopUsersQuery = Body(...), session: AsyncSession = Depends(get_session)):
    return await _top_users_impl(body, session)


async def _top_users_impl(body: TopUsersQuery, session: AsyncSession):
    df = await _events_df(session, body.start, body.end, limit=200_000)
    if df.empty:
        return JSONResponse(content={"users": [], "counts": {}, "group_field": body.group_field, "total_unique": 0})
    if body.where:
        df = _apply_where(df, body.where)

    group_fields_raw = body.group_field or "user_id"
    group_fields = [f.strip() for f in group_fields_raw.split(",") if f.strip()]
    valid_fields = []
    for gf in group_fields:
        if gf in df.columns:
            valid_fields.append(gf)
        elif gf.startswith("meta.") and "meta" in df.columns:
            meta_key = gf.split(".", 1)[1]
            df[gf] = df["meta"].apply(lambda m: m.get(meta_key) if isinstance(m, dict) else None)
            valid_fields.append(gf)
    if not valid_fields:
        valid_fields = ["user_id"] if "user_id" in df.columns else [df.columns[0]]

    if len(valid_fields) > 1:
        df["__group_key"] = df[valid_fields].astype(str).agg(" | ".join, axis=1)
        group_col = "__group_key"
    else:
        group_col = valid_fields[0]

    counts = df[group_col].value_counts().head(body.n)
    top_users = counts.index.tolist()
    counts_dict = {str(k): int(v) for k, v in counts.items()}

    return JSONResponse(content={
        "users": [str(u) for u in top_users if pd.notna(u)],
        "counts": counts_dict, "group_field": group_fields_raw,
        "group_fields": valid_fields, "total_unique": int(df[group_col].nunique()),
    })


# ════════════════════════��═════════════════════════════════════════════
# Existing ingest / query / export endpoints (unchanged)
# ══════════════════════════════════════════════════════════════════════

@app.post("/ingest/events")
async def ingest_events(items: List[EventIn], session: AsyncSession = Depends(get_session)) -> Dict[str, int]:
    to_add = [Event.from_in(item) for item in items]
    session.add_all(to_add)
    await session.commit()
    ingested_events.inc(len(to_add))
    return {"ingested": len(to_add)}


@app.post("/ingest/entities")
async def ingest_entities(payload: EntitiesIn, session: AsyncSession = Depends(get_session)) -> Dict[str, int]:
    added_e, added_l = 0, 0
    for e in payload.entities or []:
        existing = await session.scalar(select(Entity).where(Entity.type == e.type, Entity.key == e.key))
        if existing:
            existing.props = e.props or {}
        else:
            session.add(Entity(type=e.type, key=e.key, props=e.props or {}))
        added_e += 1
    await session.flush()
    key_to_id = {(ent.type, ent.key): ent.id for ent in (await session.scalars(select(Entity))).all()}
    for l in payload.links or []:
        src_id = (
            key_to_id.get(("USER", l.src_key)) or key_to_id.get(("ACCOUNT", l.src_key))
            or key_to_id.get(("DEVICE", l.src_key)) or key_to_id.get(("CARD", l.src_key))
            or key_to_id.get(("IP", l.src_key))
        )
        dst_id = (
            key_to_id.get(("USER", l.dst_key)) or key_to_id.get(("ACCOUNT", l.dst_key))
            or key_to_id.get(("DEVICE", l.dst_key)) or key_to_id.get(("CARD", l.dst_key))
            or key_to_id.get(("IP", l.dst_key))
        )
        if src_id and dst_id:
            session.add(Link(src_entity_id=src_id, dst_entity_id=dst_id, relation=l.relation, props=l.props or {}))
            added_l += 1
    await session.commit()
    return {"entities": added_e, "links": added_l}


@app.get("/query/events", response_model=Dict[str, List[EventOut]])
async def query_events(
    start: Optional[datetime] = None, end: Optional[datetime] = None,
    user: Optional[str] = None, account: Optional[str] = None,
    source: Optional[str] = None, ip: Optional[str] = None,
    device: Optional[str] = None,
    limit: int = Query(5000, ge=1, le=50_000),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, List[EventOut]]:
    t0 = time.perf_counter()
    filters = []
    if start:
        filters.append(Event.ts >= start)
    if end:
        filters.append(Event.ts <= end)
    if user:
        filters.append(Event.user_id == user)
    if account:
        filters.append(Event.account_id == account)
    if source:
        filters.append(Event.source == source)
    if ip:
        filters.append(Event.ip == ip)
    if device:
        filters.append(Event.device_id == device)
    stmt = select(Event).where(and_(*filters)) if filters else select(Event)
    stmt = stmt.order_by(Event.ts).limit(limit)
    rows = (await session.scalars(stmt)).all()
    items = [e.to_out() for e in rows]
    query_latency.observe(time.perf_counter() - t0)
    return {"items": items}


class ExportRequest(BaseModel):
    selection: Dict[str, Any] = Field(default_factory=dict)


@app.post("/export/bundle")
async def export_bundle(req: ExportRequest) -> Response:
    bundle = io.BytesIO()
    with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("selection.json", orjson.dumps(req.selection).decode())
    bundle.seek(0)
    headers = {"Content-Disposition": 'attachment; filename="lizard_bundle.zip"'}
    return Response(content=bundle.read(), media_type="application/zip", headers=headers)


# ══════════════════════════════════════════════════════════════════════
# Router registration
# ══════════════════════════════════════════════════════════════════════

try:
    from app.rules_api import router as rules_router
    app.include_router(rules_router)
    log.info("rules_engine_loaded", status="ok")
except ImportError as e:
    log.warning("rules_engine_not_loaded", error=str(e))

try:
    from app.mapping_api import router as mapping_router
    app.include_router(mapping_router)
    log.info("mapping_api_loaded", status="ok")
except ImportError as e:
    log.warning("mapping_api_not_loaded", error=str(e))

try:
    from app.workbench_api import router as workbench_router
    app.include_router(workbench_router)
    log.info("workbench_api_loaded", status="ok")
except ImportError as e:
    log.warning("workbench_api_not_loaded", error=str(e))

try:
    from app.cloud_api import router as cloud_router
    app.include_router(cloud_router)
    log.info("cloud_api_loaded", status="ok")
except ImportError as e:
    log.warning("cloud_api_not_loaded", error=str(e))


# ══════════════════════════════════════════════════════════════════════
# Administrative endpoints
# ══════════════════════════════════════════════════════════════════════

@app.delete("/events")
async def delete_all_events(session: AsyncSession = Depends(get_session)) -> Dict[str, Any]:
    from sqlalchemy import delete, func
    count_result = await session.execute(select(func.count(Event.id)))
    count_before = count_result.scalar() or 0
    await session.execute(delete(Event))
    await session.commit()
    log.info("deleted_all_events", count=count_before)
    return {"deleted": count_before, "message": f"Deleted {count_before} events"}


@app.delete("/events/source/{source_name}")
async def delete_events_by_source(source_name: str, session: AsyncSession = Depends(get_session)) -> Dict[str, Any]:
    from sqlalchemy import delete, func
    count_result = await session.execute(select(func.count(Event.id)).where(Event.source == source_name))
    count_before = count_result.scalar() or 0
    await session.execute(delete(Event).where(Event.source == source_name))
    await session.commit()
    log.info("deleted_events_by_source", source=source_name, count=count_before)
    return {"source": source_name, "deleted": count_before, "message": f"Deleted {count_before} events from source '{source_name}'"}


@app.get("/events/sources")
async def list_event_sources(session: AsyncSession = Depends(get_session)) -> List[Dict[str, Any]]:
    from sqlalchemy import func
    result = await session.execute(
        select(Event.source, func.count(Event.id).label("count"))
        .group_by(Event.source).order_by(func.count(Event.id).desc())
    )
    return [{"source": row.source, "count": row.count} for row in result.all()]
