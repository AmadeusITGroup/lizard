# path: app/rules_api.py
"""
API endpoints for Rules Engine management.
This is a complete, standalone router to be included in main.py
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional
from datetime import datetime
import uuid

from fastapi import APIRouter, Depends, HTTPException, Body, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from analytics.rules_engine import (
    RulesEngine,
    parse_rule,
    BUILTIN_RULES,
    create_default_engine,
)

router = APIRouter(prefix="/rules", tags=["Rules Engine"])

# ============================================================
# In-memory storage (will be replaced with DB when models are migrated)
# ============================================================
_rules_store: Dict[str, Dict[str, Any]] = {}


def _init_store():
    """Initialize with built-in rules if empty."""
    global _rules_store
    if not _rules_store:
        for rule in BUILTIN_RULES:
            _rules_store[rule["id"]] = {
                **rule,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
            }


_init_store()


# ============================================================
# Pydantic Models
# ============================================================

class ConditionSchema(BaseModel):
    field: str
    op: str
    value: Any = None
    value2: Any = None


class ConditionGroupSchema(BaseModel):
    operator: str = "AND"
    rules: List[Any] = Field(default_factory=list)


class RuleCreateRequest(BaseModel):
    id: Optional[str] = None
    name: str
    description: str = ""
    severity: str = "medium"
    enabled: bool = True
    conditions: Dict[str, Any]
    tags: List[str] = Field(default_factory=list)
    actions: List[str] = Field(default_factory=lambda: ["flag"])
    score_contribution: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RuleUpdateRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    severity: Optional[str] = None
    enabled: Optional[bool] = None
    conditions: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    actions: Optional[List[str]] = None
    score_contribution: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None


class RuleTestRequest(BaseModel):
    rule: RuleCreateRequest
    test_data: List[Dict[str, Any]]


class RuleEvaluateRequest(BaseModel):
    rule_ids: Optional[List[str]] = None
    start: str
    end: str
    where: Optional[List[Dict[str, Any]]] = None
    limit: int = 10000


# ============================================================
# CRUD Endpoints
# ============================================================

@router.get("/")
async def list_rules(
        enabled_only: bool = Query(False),
        tag: Optional[str] = Query(None),
        severity: Optional[str] = Query(None),
) -> List[Dict[str, Any]]:
    """List all rules with optional filters."""
    result = list(_rules_store.values())

    if enabled_only:
        result = [r for r in result if r.get("enabled", True)]
    if severity:
        result = [r for r in result if r.get("severity") == severity.lower()]
    if tag:
        result = [r for r in result if tag in r.get("tags", [])]

    return result


@router.get("/builtins")
async def list_builtin_rules() -> List[Dict[str, Any]]:
    """List built-in rule templates."""
    return BUILTIN_RULES


@router.post("/")
async def create_rule(rule: RuleCreateRequest) -> Dict[str, Any]:
    """Create a new rule."""
    try:
        parsed = parse_rule(rule.model_dump())
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid rule conditions: {e}")

    rule_id = rule.id or str(uuid.uuid4())
    rule_data = {
        **rule.model_dump(),
        "id": rule_id,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }
    _rules_store[rule_id] = rule_data

    return rule_data


@router.get("/{rule_id}")
async def get_rule(rule_id: str) -> Dict[str, Any]:
    """Get a rule by ID."""
    if rule_id not in _rules_store:
        raise HTTPException(status_code=404, detail="Rule not found")
    return _rules_store[rule_id]


@router.put("/{rule_id}")
async def update_rule(rule_id: str, updates: RuleUpdateRequest) -> Dict[str, Any]:
    """Update a rule."""
    if rule_id not in _rules_store:
        raise HTTPException(status_code=404, detail="Rule not found")

    rule_data = _rules_store[rule_id]
    update_data = updates.model_dump(exclude_unset=True)

    for key, value in update_data.items():
        rule_data[key] = value

    rule_data["updated_at"] = datetime.utcnow().isoformat()

    # Validate if conditions changed
    if "conditions" in update_data:
        try:
            parse_rule(rule_data)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid rule conditions: {e}")

    _rules_store[rule_id] = rule_data
    return rule_data


@router.delete("/{rule_id}")
async def delete_rule(rule_id: str) -> Dict[str, str]:
    """Delete a rule."""
    if rule_id not in _rules_store:
        raise HTTPException(status_code=404, detail="Rule not found")

    del _rules_store[rule_id]
    return {"status": "deleted", "rule_id": rule_id}


@router.post("/{rule_id}/enable")
async def enable_rule(rule_id: str) -> Dict[str, Any]:
    """Enable a rule."""
    if rule_id not in _rules_store:
        raise HTTPException(status_code=404, detail="Rule not found")

    _rules_store[rule_id]["enabled"] = True
    _rules_store[rule_id]["updated_at"] = datetime.utcnow().isoformat()
    return {"status": "enabled", "rule_id": rule_id}


@router.post("/{rule_id}/disable")
async def disable_rule(rule_id: str) -> Dict[str, Any]:
    """Disable a rule."""
    if rule_id not in _rules_store:
        raise HTTPException(status_code=404, detail="Rule not found")

    _rules_store[rule_id]["enabled"] = False
    _rules_store[rule_id]["updated_at"] = datetime.utcnow().isoformat()
    return {"status": "disabled", "rule_id": rule_id}


# ============================================================
# Testing & Evaluation
# ============================================================

@router.post("/test")
async def test_rule(request: RuleTestRequest) -> Dict[str, Any]:
    """Test a rule against sample data without saving."""
    try:
        rule = parse_rule(request.rule.model_dump())
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid rule:  {e}")

    results = []
    for i, row in enumerate(request.test_data):
        match = rule.evaluate(row)
        results.append({
            "row_index": i,
            "matched": match is not None,
            "match_details": match,
        })

    matched_count = sum(1 for r in results if r["matched"])

    return {
        "rule_id": request.rule.id or "test",
        "rule_name": request.rule.name,
        "total_rows": len(request.test_data),
        "matched_count": matched_count,
        "match_rate": matched_count / len(request.test_data) if request.test_data else 0,
        "results": results,
    }


@router.post("/evaluate")
async def evaluate_rules_endpoint(request: RuleEvaluateRequest) -> Dict[str, Any]:
    """Evaluate rules against events - simplified version."""
    # Get enabled rules
    if request.rule_ids:
        rules = [_rules_store[rid] for rid in request.rule_ids if rid in _rules_store]
    else:
        rules = [r for r in _rules_store.values() if r.get("enabled", True)]

    if not rules:
        return {"events_processed": 0, "matches": [], "summary": {}}

    # Create engine
    engine = RulesEngine(rules)

    return {
        "message": "Rule evaluation requires event data. Use /viz/timeline or /viz/grid with rules integration.",
        "rules_loaded": len(rules),
        "rule_ids": [r["id"] for r in rules],
    }


# ============================================================
# Import/Export
# ============================================================

@router.post("/import")
async def import_rules(
        rules: List[RuleCreateRequest],
        replace: bool = Query(False, description="Replace existing rules with same ID"),
) -> Dict[str, Any]:
    """Import multiple rules."""
    imported = 0
    skipped = 0
    errors = []

    for rule_data in rules:
        try:
            rule_dict = rule_data.model_dump()
            rule_id = rule_dict.get("id") or str(uuid.uuid4())

            if rule_id in _rules_store:
                if replace:
                    del _rules_store[rule_id]
                else:
                    skipped += 1
                    continue

            # Validate
            parse_rule(rule_dict)

            # Store
            _rules_store[rule_id] = {
                **rule_dict,
                "id": rule_id,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
            }
            imported += 1

        except Exception as e:
            errors.append({"rule": rule_data.name, "error": str(e)})

    return {
        "imported": imported,
        "skipped": skipped,
        "errors": errors,
    }


@router.get("/export")
async def export_rules() -> List[Dict[str, Any]]:
    """Export all rules."""
    return list(_rules_store.values())


@router.post("/init-builtins")
async def init_builtin_rules() -> Dict[str, Any]:
    """Initialize with built-in rules."""
    imported = 0
    skipped = 0

    for rule_data in BUILTIN_RULES:
        if rule_data["id"] in _rules_store:
            skipped += 1
            continue

        _rules_store[rule_data["id"]] = {
            **rule_data,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }
        imported += 1

    return {"imported": imported, "skipped": skipped}


@router.post("/reset-builtins")
async def reset_builtin_rules() -> Dict[str, Any]:
    """Reset to only built-in rules."""
    global _rules_store
    _rules_store = {}

    for rule_data in BUILTIN_RULES:
        _rules_store[rule_data["id"]] = {
            **rule_data,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }

    return {"rules_count": len(_rules_store)}