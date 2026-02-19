# path: app/mapping_api.py
"""
API endpoints for mapping templates, validation, data preview, and custom fields.
Enhanced with persistent custom fields and data re-mapping capabilities.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional
from datetime import datetime
import uuid
import io
import json

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import pandas as pd

from mapping.ai_mapper import (
    suggest_event_mapping,
    suggest_mapping_with_scores,
    analyze_columns,
    get_target_fields,
    get_field_metadata,
    LIZARD_FIELDS,
    FIELD_METADATA,
)
from mapping.validation import (
    DataValidator,
    ValidationResult,
    BUILTIN_VALIDATION_RULES,
    get_builtin_rules,
)
from mapping.expr import evaluate_row, evaluate_dataframe, EXPRESSION_DOCS

router = APIRouter(prefix="/mapping", tags=["Data Mapping"])

# ============================================================
# In-memory storage (replace with DB in production)
# ============================================================
_templates_store: Dict[str, Dict[str, Any]] = {}
_ingestion_logs: List[Dict[str, Any]] = []
_custom_fields: Dict[str, Dict[str, Any]] = {}  # NEW: Custom target fields


# ============================================================
# Custom Fields Management
# ============================================================

def get_all_target_fields() -> List[str]:
    """Get all target fields including custom ones."""
    all_fields = list(LIZARD_FIELDS)
    for field_name in _custom_fields:
        if field_name not in all_fields:
            all_fields.append(field_name)
    return all_fields


def get_all_field_metadata() -> Dict[str, Dict[str, Any]]:
    """Get metadata for all fields including custom ones."""
    all_metadata = dict(FIELD_METADATA)
    all_metadata.update(_custom_fields)
    return all_metadata


# ============================================================
# Initialize built-in templates
# ============================================================

def _init_builtin_templates():
    """Initialize with built-in mapping templates including fraud scenario templates."""
    builtins = [
        # Fraud Scenario - Auth Events
        {
            "id": "fraud-auth-events",
            "name": "Fraud Scenario - Auth Events",
            "description": "Mapping for authentication events from fraud scenario generator (auth_events.csv)",
            "category": "auth",
            "source_type": "csv",
            "tags": ["fraud", "auth", "vpn", "security", "scenario"],
            "is_builtin": True,
            "sample_columns": [
                "ts", "event_type", "user_id", "device_id", "ip",
                "geo_lat", "geo_lon", "country", "city", "user_agent",
                "session_id", "is_vpn", "is_fraud_indicator", "failure_reason"
            ],
            "mapping": {
                "ts": "ts",
                "event_type": "event_type",
                "user_id": "user_id",
                "device_id": "device_id",
                "ip": "ip",
                "geo_lat": "geo_lat",
                "geo_lon": "geo_lon",
                "country": "country",
                "city": "city",
                "user_agent": "user_agent",
                "session_id": "session_id",
                "is_vpn": "is_vpn",
                "is_fraud_indicator": "is_fraud_indicator",
                "failure_reason": "failure_reason",
                "source_value": "auth_events",
            },
            "expressions": {
                "geo_lat": "try_float",
                "geo_lon": "try_float",
                "is_vpn": "bool",
                "is_fraud_indicator": "bool",
                "country": "upper",
            },
            "validation_rules": [
                {"name": "Timestamp Required", "target_field": "ts", "rule_type": "required", "on_failure": "reject",
                 "severity": "error"},
                {"name": "User ID Required", "target_field": "user_id", "rule_type": "not_empty", "on_failure": "warn",
                 "severity": "warning"},
            ],
        },
        # Fraud Scenario - Ticket Events
        {
            "id": "fraud-ticket-events",
            "name": "Fraud Scenario - Ticket Events",
            "description": "Mapping for ticketing events from fraud scenario generator (ticket_events.csv)",
            "category": "travel",
            "source_type": "csv",
            "tags": ["fraud", "ticketing", "travel", "payment", "scenario"],
            "is_builtin": True,
            "sample_columns": [
            "ts", "event_type", "user_id", "device_id", "ip",
            "geo_lat", "geo_lon", "country", "city", "office_id",
            "user_sign", "organization", "pnr", "carrier", "origin",
            "dest", "tkt_number", "status", "pos_country", "card_country",
            "card_hash", "advance_hours", "stay_nights", "legs",
            "amount", "currency", "fop_type", "fop_name", "fop_subtype",
            "card_last4", "card_bin", "is_fraud_indicator", "failure_reason"
            ],
            "mapping": {
                    "ts": "ts",
                    "event_type": "event_type",
                    "user_id": "user_id",
                    "device_id": "device_id",
                    "ip": "ip",
                    "geo_lat": "geo_lat",
                    "geo_lon": "geo_lon",
                    "country": "country",
                    "city": "city",
                    # ALL TICKET FIELDS SHOULD GO HERE AS WELL:
                    "office_id": "office_id",
                    "user_sign":  "user_sign",
                    "organization":  "organization",
                    "pnr": "pnr",
                    "carrier": "carrier",
                    "origin":  "origin",
                    "dest": "dest",
                    "tkt_number": "tkt_number",
                    "status": "status",
                    "pos_country": "pos_country",
                    "card_country": "card_country",
                    "card_hash": "card_hash",
                    "advance_hours": "advance_hours",
                    "stay_nights": "stay_nights",
                    "legs": "legs",
                    "amount": "amount",
                    "currency": "currency",
                    "fop_type": "fop_type",
                    "fop_name": "fop_name",
                    "fop_subtype": "fop_subtype",
                    "card_last4": "card_last4",
                    "card_bin": "card_bin",
                    "is_fraud_indicator": "is_fraud_indicator",
                    "failure_reason": "failure_reason",
                    "source_value": "ticket_events",
                },
            "expressions": {
                "geo_lat": "try_float",
                "geo_lon": "try_float",
                "amount": "try_float",
                "advance_hours": "try_float",
                "stay_nights": "try_int",
                "is_fraud_indicator": "bool",
                "carrier": "upper",
                "origin": "upper",
                "dest": "upper",
                "currency": "upper",
                "country": "upper",
                "pos_country": "upper",
                "card_country": "upper"
            },
            "validation_rules": [
                {"name": "Timestamp Required", "target_field": "ts", "rule_type": "required", "on_failure": "reject",
                 "severity": "error"},
                {"name": "Amount Non-Negative", "target_field": "amount", "rule_type": "range", "config": {"min": 0},
                 "on_failure": "warn", "severity": "warning"}
            ]
        },
        # Generic Auth Logs
        {
            "id": "auth-logs-generic",
            "name": "Authentication Logs (Generic)",
            "description": "Generic authentication/login event logs with user, IP, and device info",
            "category": "auth",
            "source_type": "csv",
            "tags": ["auth", "login", "security", "generic"],
            "is_builtin": True,
            "sample_columns": ["timestamp", "username", "event", "ip_address", "device_id", "user_agent", "status",
                               "country", "city"],
            "mapping": {
                "ts": "timestamp",
                "event_type": "event",
                "user_id": "username",
                "ip": "ip_address",
                "device_id": "device_id",
                "user_agent": "user_agent",
                "status": "status",
                "country": "country",
                "city": "city",
                "source_value": "auth_logs",
            },
            "expressions": {"country": "upper"},
            "validation_rules": [
                {"name": "Timestamp Required", "target_field": "ts", "rule_type": "required", "on_failure": "reject",
                 "severity": "error"},
            ],
        },
        # Travel Ticketing
        {
            "id": "travel-amadeus",
            "name": "Amadeus Travel Ticketing",
            "description": "Mapping for Amadeus GDS ticketing data with PNR, carriers, and office IDs",
            "category": "travel",
            "source_type": "csv",
            "tags": ["amadeus", "gds", "ticketing", "travel"],
            "is_builtin": True,
            "sample_columns": ["TIMESTAMP", "EVENT_TYPE", "USER_SIGN", "PNR", "TKT_NUMBER", "CARRIER", "ORIGIN", "DEST",
                               "OFFICE_ID", "FARE_AMOUNT", "CURRENCY"],
            "mapping": {
                "ts": "TIMESTAMP",
                "event_type": "EVENT_TYPE",
                "user_id": "USER_SIGN",
                "pnr": "PNR",
                "tkt_number": "TKT_NUMBER",
                "carrier": "CARRIER",
                "origin": "ORIGIN",
                "dest": "DEST",
                "office_id": "OFFICE_ID",
                "amount": "FARE_AMOUNT",
                "currency": "CURRENCY",
                "source_value": "amadeus_ticketing",
            },
            "expressions": {"amount": "try_float", "carrier": "upper", "origin": "upper", "dest": "upper"},
            "validation_rules": [
                {"name": "Timestamp Required", "target_field": "ts", "rule_type": "required", "on_failure": "reject",
                 "severity": "error"},
            ],
        },
        # Payment Transactions
        {
            "id": "payment-transactions",
            "name": "Payment Transactions",
            "description": "Payment/transaction data with card, amount, and merchant info",
            "category": "payment",
            "source_type": "csv",
            "tags": ["payment", "transaction", "card", "fraud"],
            "is_builtin": True,
            "sample_columns": ["transaction_id", "timestamp", "user_id", "account_id", "card_hash", "amount",
                               "currency", "merchant_id", "ip_address", "country"],
            "mapping": {
                "ts": "timestamp",
                "event_type": "transaction_type",
                "user_id": "user_id",
                "account_id": "account_id",
                "card_hash": "card_hash",
                "amount": "amount",
                "currency": "currency",
                "ip": "ip_address",
                "country": "country",
                "order_id": "transaction_id",
                "source_value": "payment_transactions",
            },
            "expressions": {"amount": "try_float", "currency": "upper", "country": "upper"},
            "validation_rules": [
                {"name": "Timestamp Required", "target_field": "ts", "rule_type": "required", "on_failure": "reject",
                 "severity": "error"},
            ],
        },
        # Generic Events
        {
            "id": "generic-events",
            "name": "Generic Event Log",
            "description": "Flexible mapping for generic timestamped event data",
            "category": "general",
            "source_type": "csv",
            "tags": ["generic", "events", "flexible"],
            "is_builtin": True,
            "sample_columns": ["time", "type", "user", "data", "source"],
            "mapping": {
                "ts": "time",
                "event_type": "type",
                "user_id": "user",
                "source_value": "generic_events",
            },
            "expressions": {},
            "validation_rules": [
                {"name": "Timestamp Required", "target_field": "ts", "rule_type": "required", "on_failure": "reject",
                 "severity": "error"},
            ],
        },
    ]

    for tpl in builtins:
        tpl["created_at"] = datetime.utcnow().isoformat()
        tpl["updated_at"] = datetime.utcnow().isoformat()
        tpl["use_count"] = 0
        tpl["last_used_at"] = None
        tpl["is_active"] = True
        tpl["created_by"] = "system"
        _templates_store[tpl["id"]] = tpl


_init_builtin_templates()


# ============================================================
# Pydantic Models
# ============================================================

class MappingTemplateCreate(BaseModel):
    name: str
    description: str = ""
    mapping: Dict[str, Any]
    expressions: Dict[str, Any] = Field(default_factory=dict)
    source_type: str = "csv"
    category: str = "general"
    tags: List[str] = Field(default_factory=list)
    sample_columns: List[str] = Field(default_factory=list)
    validation_rules: List[Dict[str, Any]] = Field(default_factory=list)


class MappingTemplateUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    mapping: Optional[Dict[str, Any]] = None
    expressions: Optional[Dict[str, Any]] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    sample_columns: Optional[List[str]] = None
    validation_rules: Optional[List[Dict[str, Any]]] = None
    is_active: Optional[bool] = None


class CustomFieldCreate(BaseModel):
    name: str
    type: str = "string"  # string, number, boolean, datetime, json
    description: str = ""
    required: bool = False


class RemapRequest(BaseModel):
    source_name: str
    mapping: Dict[str, Any]
    expressions: Dict[str, Any] = Field(default_factory=dict)
    start: Optional[str] = None
    end: Optional[str] = None
    dry_run: bool = False


# ============================================================
# Custom Fields Endpoints
# ============================================================

@router.get("/custom-fields")
async def list_custom_fields() -> List[Dict[str, Any]]:
    """List all custom target fields."""
    return [
        {"name": name, **info}
        for name, info in _custom_fields.items()
    ]


@router.post("/custom-fields")
async def create_custom_field(field: CustomFieldCreate) -> Dict[str, Any]:
    """Create a new custom target field."""
    # Validate field name
    field_name = field.name.lower().strip().replace(" ", "_").replace("-", "_")
    if not field_name.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid field name. Use only letters, numbers, and underscores.")

    # Check if already exists
    if field_name in LIZARD_FIELDS or field_name in _custom_fields:
        raise HTTPException(status_code=400, detail=f"Field '{field_name}' already exists")

    # Validate type
    valid_types = ["string", "number", "boolean", "datetime", "json"]
    if field.type not in valid_types:
        raise HTTPException(status_code=400, detail=f"Invalid type.Must be one of: {', '.join(valid_types)}")

    # Create field
    _custom_fields[field_name] = {
        "type": field.type,
        "description": field.description or f"Custom field:  {field_name}",
        "required": field.required,
        "is_custom": True,
        "created_at": datetime.utcnow().isoformat(),
    }

    return {"name": field_name, **_custom_fields[field_name]}


@router.put("/custom-fields/{field_name}")
async def update_custom_field(field_name: str, updates: Dict[str, Any]) -> Dict[str, Any]:
    """Update a custom field."""
    if field_name not in _custom_fields:
        raise HTTPException(status_code=404, detail=f"Custom field '{field_name}' not found")

    allowed_updates = {"description", "required", "type"}
    for key, value in updates.items():
        if key in allowed_updates:
            _custom_fields[field_name][key] = value

    _custom_fields[field_name]["updated_at"] = datetime.utcnow().isoformat()

    return {"name": field_name, **_custom_fields[field_name]}


@router.delete("/custom-fields/{field_name}")
async def delete_custom_field(field_name: str) -> Dict[str, str]:
    """Delete a custom field."""
    if field_name not in _custom_fields:
        raise HTTPException(status_code=404, detail=f"Custom field '{field_name}' not found")

    del _custom_fields[field_name]
    return {"status": "deleted", "field": field_name}


# ============================================================
# Template CRUD Endpoints
# ============================================================

@router.get("/templates")
async def list_templates(
        category: Optional[str] = Query(None),
        tag: Optional[str] = Query(None),
        active_only: bool = Query(True),
        search: Optional[str] = Query(None),
) -> List[Dict[str, Any]]:
    """List all mapping templates with optional filters."""
    result = list(_templates_store.values())

    if active_only:
        result = [t for t in result if t.get("is_active", True)]
    if category:
        result = [t for t in result if t.get("category") == category]
    if tag:
        result = [t for t in result if tag in t.get("tags", [])]
    if search:
        search_lower = search.lower()
        result = [t for t in result if
                  search_lower in t.get("name", "").lower() or search_lower in t.get("description", "").lower()]

    result.sort(key=lambda x: (-x.get("use_count", 0), x.get("name", "")))
    return result


@router.get("/templates/categories")
async def list_categories() -> List[Dict[str, Any]]:
    """Get list of template categories with counts."""
    categories: Dict[str, int] = {}
    for t in _templates_store.values():
        cat = t.get("category", "general")
        categories[cat] = categories.get(cat, 0) + 1
    return [{"name": k, "count": v} for k, v in sorted(categories.items())]


@router.get("/templates/{template_id}")
async def get_template(template_id: str) -> Dict[str, Any]:
    """Get a template by ID."""
    if template_id not in _templates_store:
        raise HTTPException(status_code=404, detail="Template not found")
    return _templates_store[template_id]


@router.post("/templates")
async def create_template(template: MappingTemplateCreate) -> Dict[str, Any]:
    """Create a new mapping template."""
    template_id = str(uuid.uuid4())
    template_data = {
        "id": template_id,
        **template.model_dump(),
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
        "use_count": 0,
        "last_used_at": None,
        "is_builtin": False,
        "is_active": True,
        "created_by": "user",
    }
    _templates_store[template_id] = template_data
    return template_data


@router.put("/templates/{template_id}")
async def update_template(template_id: str, updates: MappingTemplateUpdate) -> Dict[str, Any]:
    """Update a template."""
    if template_id not in _templates_store:
        raise HTTPException(status_code=404, detail="Template not found")

    template = _templates_store[template_id]
    if template.get("is_builtin"):
        raise HTTPException(status_code=400, detail="Cannot modify built-in templates. Clone it instead.")

    for key, value in updates.model_dump(exclude_unset=True).items():
        template[key] = value

    template["updated_at"] = datetime.utcnow().isoformat()
    return template


@router.delete("/templates/{template_id}")
async def delete_template(template_id: str) -> Dict[str, str]:
    """Delete a template."""
    if template_id not in _templates_store:
        raise HTTPException(status_code=404, detail="Template not found")

    template = _templates_store[template_id]
    if template.get("is_builtin"):
        raise HTTPException(status_code=400, detail="Cannot delete built-in templates")

    del _templates_store[template_id]
    return {"status": "deleted", "template_id": template_id}


@router.post("/templates/{template_id}/clone")
async def clone_template(
        template_id: str,
        new_name: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """Clone an existing template."""
    if template_id not in _templates_store:
        raise HTTPException(status_code=404, detail="Template not found")

    original = _templates_store[template_id]
    new_id = str(uuid.uuid4())

    cloned = {
        **original,
        "id": new_id,
        "name": new_name or f"{original['name']} (Copy)",
        "is_builtin": False,
        "use_count": 0,
        "last_used_at": None,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
        "created_by": "user",
    }

    _templates_store[new_id] = cloned
    return cloned


@router.post("/templates/{template_id}/use")
async def record_template_use(template_id: str) -> Dict[str, Any]:
    """Record that a template was used."""
    if template_id not in _templates_store:
        raise HTTPException(status_code=404, detail="Template not found")

    template = _templates_store[template_id]
    template["use_count"] = template.get("use_count", 0) + 1
    template["last_used_at"] = datetime.utcnow().isoformat()

    return {"template_id": template_id, "use_count": template["use_count"]}


# ============================================================
# Template Matching & Suggestion Endpoints
# ============================================================

@router.post("/templates/match")
async def match_template(
        file: UploadFile = File(...),
        threshold: float = Query(0.3),
) -> List[Dict[str, Any]]:
    """Match uploaded file columns against existing templates."""
    content = await file.read()
    filename = file.filename or "uploaded"

    try:
        if filename.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(content))
        elif filename.endswith(".json"):
            df = pd.read_json(io.BytesIO(content), lines=True)
        else:
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(io.BytesIO(content), nrows=100, encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError("Could not decode file")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {e}")

    file_columns = set(str(c).lower().strip() for c in df.columns)

    matches = []
    for template in _templates_store.values():
        if not template.get("is_active", True):
            continue

        template_columns = set(str(c).lower().strip() for c in template.get("sample_columns", []))
        mapping_sources = set(
            str(v).lower().strip()
            for v in template.get("mapping", {}).values()
            if v and not str(v).startswith("__") and v != template.get("mapping", {}).get("source_value")
        )

        expected_columns = template_columns | mapping_sources
        if not expected_columns:
            continue

        intersection = len(file_columns & expected_columns)
        union = len(file_columns | expected_columns)
        jaccard_score = intersection / union if union > 0 else 0

        mapping_found = len(file_columns & mapping_sources)
        mapping_total = len(mapping_sources)
        mapping_coverage = mapping_found / mapping_total if mapping_total > 0 else 0

        score = (jaccard_score * 0.4) + (mapping_coverage * 0.6)

        if score >= threshold:
            matches.append({
                "template_id": template["id"],
                "template_name": template["name"],
                "category": template.get("category"),
                "description": template.get("description", "")[:100],
                "score": round(score, 3),
                "columns_matched": sorted(list(file_columns & expected_columns)),
                "columns_missing": sorted(list(expected_columns - file_columns)),
            })

    matches.sort(key=lambda x: -x["score"])
    return matches


@router.post("/templates/suggest")
async def suggest_mapping_endpoint(
        file: UploadFile = File(...),
        engine: str = Query("heuristic"),
) -> Dict[str, Any]:
    """Auto-suggest a mapping for uploaded file."""
    content = await file.read()
    filename = file.filename or "uploaded"

    try:
        if filename.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(content))
        elif filename.endswith(".json"):
            df = pd.read_json(io.BytesIO(content), lines=True)
        else:
            df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {e}")

    mapping = suggest_event_mapping(df.head(100), engine=engine)
    expressions = mapping.pop("__expr__", {})
    candidates = suggest_mapping_with_scores(df.head(100))
    column_analysis = analyze_columns(df.head(100))

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


# ============================================================
# Preview & Validation Endpoints
# ============================================================

@router.post("/preview")
async def preview_mapping(
        file: UploadFile = File(...),
        template_id: Optional[str] = Form(None),
        mapping_json: Optional[str] = Form(None),
        expressions_json: Optional[str] = Form(None),
        sample_rows: int = Form(25),
) -> Dict[str, Any]:
    """Preview how a file would be mapped."""
    content = await file.read()
    filename = file.filename or "uploaded"

    try:
        if filename.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(content))
        elif filename.endswith(".json"):
            df = pd.read_json(io.BytesIO(content), lines=True)
        else:
            df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {e}")

    validation_rules = []
    template_name = None
    template_used_id = None

    if template_id and template_id in _templates_store:
        template = _templates_store[template_id]
        mapping = template.get("mapping", {})
        expressions = template.get("expressions", {})
        validation_rules = template.get("validation_rules", [])
        template_name = template.get("name")
        template_used_id = template_id

        template["use_count"] = template.get("use_count", 0) + 1
        template["last_used_at"] = datetime.utcnow().isoformat()
    elif mapping_json:
        try:
            mapping = json.loads(mapping_json)
            expressions = json.loads(expressions_json) if expressions_json else {}
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Invalid mapping JSON: {e}")
    else:
        mapping = suggest_event_mapping(df.head(50), engine="heuristic")
        expressions = mapping.pop("__expr__", {})

    full_mapping = {**mapping, "__expr__": expressions}

    sample_df = df.head(sample_rows).copy()
    mapped_rows = []

    for idx, row in sample_df.iterrows():
        row_dict = row.to_dict()
        mapped_row = evaluate_row(row_dict, full_mapping)
        mapped_row["__row_index__"] = idx
        mapped_rows.append(mapped_row)

    validation_result = None
    if validation_rules:
        mapped_df = pd.DataFrame([{k: v for k, v in r.items() if not k.startswith("__")} for r in mapped_rows])
        validator = DataValidator(validation_rules)
        _, val_result = validator.validate(mapped_df)
        validation_result = val_result.to_dict()

    return {
        "filename": filename,
        "total_rows": len(df),
        "sample_rows": len(mapped_rows),
        "columns_source": list(df.columns),
        "columns_mapped": [f for f in mapping.keys() if not f.startswith("__")],
        "mapping": mapping,
        "expressions": expressions,
        "template_used": template_name,
        "template_id": template_used_id,
        "sample": mapped_rows,
        "validation": validation_result,
    }


# ============================================================
# Re-mapping Existing Data Endpoints
# ============================================================

@router.post("/remap")
async def remap_existing_data(request: RemapRequest) -> Dict[str, Any]:
    """
    Re-map existing data in the database with a new mapping.
    This allows changing how data is interpreted after import.
    """
    from sqlalchemy import select, update
    from sqlalchemy.ext.asyncio import AsyncSession

    # This would need to be implemented based on your database setup
    # For now, return a placeholder response

    if request.dry_run:
        return {
            "status": "dry_run",
            "source_name": request.source_name,
            "mapping": request.mapping,
            "expressions": request.expressions,
            "message": "Dry run completed. No changes made.",
            "affected_rows_estimate": 0,  # Would be calculated from DB
        }

    # In a real implementation, this would:
    # 1.Query all events with the given source_name
    # 2.Re-apply the mapping to the raw_json field
    # 3.Update the event fields accordingly

    return {
        "status": "not_implemented",
        "message": "Re-mapping requires database integration. See /remap/preview for mapping preview.",
    }


@router.post("/remap/preview")
async def preview_remap(
        source_name: str = Form(...),
        mapping_json: str = Form(...),
        limit: int = Form(10),
) -> Dict[str, Any]:
    """
    Preview how existing data would look with a new mapping.
    """
    try:
        mapping = json.loads(mapping_json)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid mapping JSON: {e}")

    # This would query the database for sample rows
    # For now, return a placeholder
    return {
        "source_name": source_name,
        "mapping": mapping,
        "sample_count": 0,
        "preview_rows": [],
        "message": "Preview requires database integration.",
    }


# ============================================================
# Ingestion Log Endpoints
# ============================================================

@router.get("/ingestion-logs")
async def list_ingestion_logs(
        limit: int = Query(50, ge=1, le=500),
        status: Optional[str] = Query(None),
        source_name: Optional[str] = Query(None),
) -> List[Dict[str, Any]]:
    """Get recent ingestion logs."""
    result = _ingestion_logs.copy()
    if status:
        result = [l for l in result if l.get("status") == status]
    if source_name:
        result = [l for l in result if l.get("source_name") == source_name]
    result.sort(key=lambda x: x.get("started_at", ""), reverse=True)
    return result[:limit]


@router.get("/ingestion-logs/{log_id}")
async def get_ingestion_log(log_id: str) -> Dict[str, Any]:
    """Get a specific ingestion log."""
    for log in _ingestion_logs:
        if log.get("id") == log_id:
            return log
    raise HTTPException(status_code=404, detail="Ingestion log not found")


@router.post("/ingestion-logs")
async def create_ingestion_log(
        filename: str = Form(...),
        source_name: str = Form(...),
        template_id: Optional[str] = Form(None),
        template_name: Optional[str] = Form(None),
        mapping_used: Optional[str] = Form(None),
) -> Dict[str, Any]:
    """Create a new ingestion log entry."""
    log_id = str(uuid.uuid4())

    mapping_dict = {}
    if mapping_used:
        try:
            mapping_dict = json.loads(mapping_used)
        except:
            pass

    log = {
        "id": log_id,
        "filename": filename,
        "source_name": source_name,
        "template_id": template_id,
        "template_name": template_name,
        "mapping_used": mapping_dict,
        "status": "pending",
        "rows_total": 0,
        "rows_ingested": 0,
        "rows_rejected": 0,
        "started_at": datetime.utcnow().isoformat(),
        "completed_at": None,
    }
    _ingestion_logs.append(log)
    return log


@router.put("/ingestion-logs/{log_id}")
async def update_ingestion_log(log_id: str, updates: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Update an ingestion log."""
    for log in _ingestion_logs:
        if log.get("id") == log_id:
            log.update(updates)
            return log
    raise HTTPException(status_code=404, detail="Ingestion log not found")


# ============================================================
# Field & Schema Info Endpoints
# ============================================================

@router.get("/fields")
async def get_fields_endpoint() -> Dict[str, Any]:
    """Get list of all target fields (built-in + custom) with descriptions."""
    return {
        "fields": get_all_target_fields(),
        "field_info": get_all_field_metadata(),
    }


@router.get("/fields/{field_name}")
async def get_field_info(field_name: str) -> Dict[str, Any]:
    """Get detailed info for a specific field."""
    all_metadata = get_all_field_metadata()
    if field_name not in all_metadata:
        raise HTTPException(status_code=404, detail=f"Field '{field_name}' not found")

    return {
        "name": field_name,
        **all_metadata[field_name],
    }


@router.get("/expressions")
async def get_expression_docs() -> Dict[str, Any]:
    """Get documentation for available expressions."""
    return EXPRESSION_DOCS


@router.get("/validation-rules/builtins")
async def get_builtin_validation_rules() -> List[Dict[str, Any]]:
    """Get built-in validation rules."""
    return get_builtin_rules()


@router.get("/validation-rules/types")
async def get_validation_rule_types() -> Dict[str, Any]:
    """Get available validation rule types and options."""
    return {
        "types": {
            "required": "Field must exist and not be null",
            "not_null": "Field must not be null (if present)",
            "not_empty": "Field must not be null or empty string",
            "type_check": "Field must be of specified type",
            "range": "Numeric field must be within min/max range",
            "regex": "Field must match regex pattern",
            "enum": "Field must be one of allowed values",
            "length": "String field must have length within min/max",
            "date_format": "Field must match date format",
        },
        "on_failure_options": {
            "reject": "Reject the entire row",
            "warn": "Log warning but keep row",
            "fix": "Try to fix the value",
            "default": "Replace with default value",
            "skip_field": "Set field to null",
        },
        "severity_levels": ["info", "warning", "error"],
    }


# Add these endpoints to app/mapping_api.py

@router.delete("/ingestion-logs")
async def clear_ingestion_logs() -> Dict[str, Any]:
    """Clear all ingestion logs."""
    global _ingestion_logs

    count = len(_ingestion_logs)
    _ingestion_logs.clear()

    return {
        "deleted": count,
        "message": f"Cleared {count} ingestion logs"
    }


@router.delete("/ingestion-logs/{log_id}")
async def delete_ingestion_log(log_id: str) -> Dict[str, Any]:
    """Delete a specific ingestion log."""
    global _ingestion_logs

    for i, log in enumerate(_ingestion_logs):
        if log.get("id") == log_id:
            _ingestion_logs.pop(i)
            return {"deleted": log_id, "message": "Ingestion log deleted"}

    raise HTTPException(status_code=404, detail="Ingestion log not found")