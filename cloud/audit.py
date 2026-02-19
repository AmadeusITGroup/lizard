# cloud/audit.py
"""
Structured audit logging for LIZARD Cloud operations.

Records every cloud operation in an in-memory ring buffer with optional
persistence to the database or filesystem.

Audit events include:
  - Configuration changes (mode switch, connection add/remove)
  - Connection tests (success/failure)
  - Data exports (to blob/DBFS)
  - Analytics runs (anomaly detection, clustering)
  - Cluster operations (start/stop)

Each entry captures: who, what, when, outcome, and context.
"""
from __future__ import annotations

import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Literal, Optional

import structlog
from pydantic import BaseModel, Field

log = structlog.get_logger(__name__)


# ── Audit entry model ────────────────────────────────────────────────


class AuditEntry(BaseModel):
    """A single audit log entry."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    operation: str  # e.g. "config_update", "test_connection", "export", "analytics"
    category: Literal[
        "config", "connection", "export", "analytics", "cluster", "system"
    ] = "system"
    status: Literal["ok", "error", "partial"] = "ok"
    user: Optional[str] = None  # user/principal who performed the action
    detail: Dict[str, Any] = Field(default_factory=dict)
    duration_ms: Optional[float] = None
    error: Optional[str] = None


# ── Audit logger ─────────────────────────────────────────────────────

_MAX_ENTRIES = 1000  # Ring buffer size

_lock = threading.Lock()
_entries: Deque[AuditEntry] = deque(maxlen=_MAX_ENTRIES)


def record(
    operation: str,
    *,
    category: str = "system",
    status: str = "ok",
    user: Optional[str] = None,
    detail: Optional[Dict[str, Any]] = None,
    duration_ms: Optional[float] = None,
    error: Optional[str] = None,
) -> AuditEntry:
    """
    Record an audit event.

    Thread-safe. Returns the created entry.
    """
    entry = AuditEntry(
        operation=operation,
        category=category,
        status=status,
        user=user,
        detail=detail or {},
        duration_ms=duration_ms,
        error=error,
    )

    with _lock:
        _entries.append(entry)

    log.info(
        "audit_recorded",
        operation=operation,
        category=category,
        status=status,
        duration_ms=duration_ms,
    )

    return entry


def get_entries(
    *,
    limit: int = 100,
    offset: int = 0,
    category: Optional[str] = None,
    status: Optional[str] = None,
    operation: Optional[str] = None,
) -> List[AuditEntry]:
    """
    Retrieve audit entries with optional filtering.

    Returns newest-first (reverse chronological).
    """
    with _lock:
        # Newest first
        all_entries = list(reversed(_entries))

    # Apply filters
    if category:
        all_entries = [e for e in all_entries if e.category == category]
    if status:
        all_entries = [e for e in all_entries if e.status == status]
    if operation:
        all_entries = [e for e in all_entries if e.operation == operation]

    # Paginate
    return all_entries[offset: offset + limit]


def get_stats() -> Dict[str, Any]:
    """
    Return summary statistics of audit log.
    """
    with _lock:
        entries = list(_entries)

    if not entries:
        return {
            "total_entries": 0,
            "by_category": {},
            "by_status": {},
            "by_operation": {},
            "oldest_entry": None,
            "newest_entry": None,
        }

    by_category: Dict[str, int] = {}
    by_status: Dict[str, int] = {}
    by_operation: Dict[str, int] = {}

    for e in entries:
        by_category[e.category] = by_category.get(e.category, 0) + 1
        by_status[e.status] = by_status.get(e.status, 0) + 1
        by_operation[e.operation] = by_operation.get(e.operation, 0) + 1

    return {
        "total_entries": len(entries),
        "by_category": by_category,
        "by_status": by_status,
        "by_operation": by_operation,
        "oldest_entry": entries[0].timestamp,
        "newest_entry": entries[-1].timestamp,
    }


def clear() -> int:
    """Clear all audit entries. Returns count cleared."""
    with _lock:
        count = len(_entries)
        _entries.clear()
    return count


# ── Context manager for timed operations ─────────────────────────────


class audit_operation:
    """
    Context manager that automatically records an audit entry
    with timing and error capture.

    Usage::

        with audit_operation("export", category="export", detail={"format": "csv"}) as ctx:
            # ... do work ...
            ctx["rows"] = 1000  # add to detail
    """

    def __init__(
        self,
        operation: str,
        *,
        category: str = "system",
        user: Optional[str] = None,
        detail: Optional[Dict[str, Any]] = None,
    ):
        self.operation = operation
        self.category = category
        self.user = user
        self.detail = detail or {}
        self._t0: float = 0

    def __enter__(self) -> Dict[str, Any]:
        self._t0 = time.perf_counter()
        return self.detail

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed_ms = (time.perf_counter() - self._t0) * 1000

        if exc_type is not None:
            record(
                self.operation,
                category=self.category,
                status="error",
                user=self.user,
                detail=self.detail,
                duration_ms=round(elapsed_ms, 1),
                error=str(exc_val),
            )
        else:
            record(
                self.operation,
                category=self.category,
                status="ok",
                user=self.user,
                detail=self.detail,
                duration_ms=round(elapsed_ms, 1),
            )

        # Don't suppress exceptions
        return False