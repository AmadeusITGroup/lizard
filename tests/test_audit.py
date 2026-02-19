# tests/test_audit.py
"""
Tests for cloud/audit.py.

Covers:
  - record(): creating entries, thread safety
  - get_entries(): filtering, pagination, ordering
  - get_stats(): summary statistics
  - clear(): clearing all entries
  - audit_operation: context manager with timing and error capture
  - Ring buffer behavior (max entries)
"""
from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest

from cloud.audit import (
    AuditEntry,
    audit_operation,
    clear,
    get_entries,
    get_stats,
    record,
    _entries,
    _lock,
)


# ── Setup: clear audit before each test ──────────────────────────────

@pytest.fixture(autouse=True)
def _clear_audit():
    """Ensure a clean audit log for every test."""
    clear()
    yield
    clear()


# ── AuditEntry model ────────────────────────────────────────────────


def test_audit_entry_defaults():
    entry = AuditEntry(operation="test_op")
    assert entry.operation == "test_op"
    assert entry.category == "system"
    assert entry.status == "ok"
    assert entry.id  # UUID generated
    assert entry.timestamp  # ISO timestamp generated
    assert entry.detail == {}
    assert entry.user is None
    assert entry.duration_ms is None
    assert entry.error is None


def test_audit_entry_custom_fields():
    entry = AuditEntry(
        operation="export",
        category="export",
        status="error",
        user="admin@corp.com",
        detail={"format": "csv", "rows": 100},
        duration_ms=42.5,
        error="Connection timeout",
    )
    assert entry.operation == "export"
    assert entry.category == "export"
    assert entry.status == "error"
    assert entry.user == "admin@corp.com"
    assert entry.detail["rows"] == 100
    assert entry.duration_ms == 42.5
    assert entry.error == "Connection timeout"


# ── record() ─────────────────────────────────────────────────────────


def test_record_basic():
    entry = record("config_update", category="config")
    assert entry.operation == "config_update"
    assert entry.category == "config"
    assert entry.status == "ok"

    entries = get_entries()
    assert len(entries) == 1
    assert entries[0].id == entry.id


def test_record_with_detail():
    entry = record(
        "test_connection",
        category="connection",
        status="error",
        detail={"connection_name": "my_ws", "steps": 3},
        duration_ms=150.5,
        error="Auth failed",
    )
    assert entry.detail["connection_name"] == "my_ws"
    assert entry.duration_ms == 150.5
    assert entry.error == "Auth failed"


def test_record_multiple():
    record("op1", category="config")
    record("op2", category="export")
    record("op3", category="analytics")

    entries = get_entries()
    assert len(entries) == 3


def test_record_with_user():
    entry = record("mode_switch", category="config", user="user@test.com")
    assert entry.user == "user@test.com"


# ── get_entries() ────────────────────────────────────────────────────


def test_get_entries_newest_first():
    record("first")
    time.sleep(0.01)
    record("second")
    time.sleep(0.01)
    record("third")

    entries = get_entries()
    assert entries[0].operation == "third"
    assert entries[2].operation == "first"


def test_get_entries_filter_by_category():
    record("a", category="config")
    record("b", category="export")
    record("c", category="config")

    entries = get_entries(category="config")
    assert len(entries) == 2
    assert all(e.category == "config" for e in entries)


def test_get_entries_filter_by_status():
    record("ok_op", status="ok")
    record("err_op", status="error")
    record("ok_op2", status="ok")

    entries = get_entries(status="error")
    assert len(entries) == 1
    assert entries[0].operation == "err_op"


def test_get_entries_filter_by_operation():
    record("export")
    record("export")
    record("config_update")

    entries = get_entries(operation="export")
    assert len(entries) == 2


def test_get_entries_pagination():
    for i in range(10):
        record(f"op_{i}")

    page1 = get_entries(limit=3, offset=0)
    page2 = get_entries(limit=3, offset=3)

    assert len(page1) == 3
    assert len(page2) == 3
    assert page1[0].id != page2[0].id


def test_get_entries_combined_filters():
    record("export", category="export", status="ok")
    record("export", category="export", status="error")
    record("config", category="config", status="ok")

    entries = get_entries(category="export", status="ok")
    assert len(entries) == 1
    assert entries[0].operation == "export"
    assert entries[0].status == "ok"


# ── get_stats() ──────────────────────────────────────────────────────


def test_get_stats_empty():
    stats = get_stats()
    assert stats["total_entries"] == 0
    assert stats["by_category"] == {}
    assert stats["oldest_entry"] is None


def test_get_stats_populated():
    record("a", category="config", status="ok")
    record("b", category="export", status="ok")
    record("c", category="export", status="error")

    stats = get_stats()
    assert stats["total_entries"] == 3
    assert stats["by_category"]["config"] == 1
    assert stats["by_category"]["export"] == 2
    assert stats["by_status"]["ok"] == 2
    assert stats["by_status"]["error"] == 1
    assert stats["by_operation"]["a"] == 1
    assert stats["oldest_entry"] is not None
    assert stats["newest_entry"] is not None


# ── clear() ──────────────────────────────────────────────────────────


def test_clear():
    record("a")
    record("b")
    assert len(get_entries()) == 2

    count = clear()
    assert count == 2
    assert len(get_entries()) == 0


def test_clear_empty():
    count = clear()
    assert count == 0


# ── Ring buffer ──────────────────────────────────────────────────────


def test_ring_buffer_evicts_old():
    """When max entries exceeded, oldest are evicted."""
    from cloud import audit

    original_max = audit._MAX_ENTRIES

    # Temporarily shrink the buffer for testing
    with _lock:
        old_entries = audit._entries
        audit._entries = type(old_entries)(maxlen=5)

    try:
        for i in range(10):
            record(f"op_{i}")

        entries = get_entries()
        assert len(entries) == 5
        # Newest first, so op_9 should be first
        assert entries[0].operation == "op_9"
        assert entries[4].operation == "op_5"
    finally:
        with _lock:
            audit._entries = old_entries


# ── audit_operation context manager ──────────────────────────────────


def test_audit_operation_success():
    with audit_operation("export", category="export", detail={"format": "csv"}) as ctx:
        ctx["rows"] = 500

    entries = get_entries()
    assert len(entries) == 1
    assert entries[0].operation == "export"
    assert entries[0].status == "ok"
    assert entries[0].detail["format"] == "csv"
    assert entries[0].detail["rows"] == 500
    assert entries[0].duration_ms is not None
    assert entries[0].duration_ms >= 0
    assert entries[0].error is None


def test_audit_operation_with_error():
    with pytest.raises(ValueError):
        with audit_operation("bad_export", category="export") as ctx:
            ctx["attempted"] = True
            raise ValueError("Something broke")

    entries = get_entries()
    assert len(entries) == 1
    assert entries[0].operation == "bad_export"
    assert entries[0].status == "error"
    assert entries[0].error == "Something broke"
    assert entries[0].detail["attempted"] is True


def test_audit_operation_measures_time():
    with audit_operation("slow_op", category="system") as ctx:
        time.sleep(0.05)

    entries = get_entries()
    assert entries[0].duration_ms >= 40  # At least ~50ms


def test_audit_operation_with_user():
    with audit_operation("admin_op", category="config", user="admin@corp.com"):
        pass

    entries = get_entries()
    assert entries[0].user == "admin@corp.com"


# ── Thread safety ────────────────────────────────────────────────────


def test_concurrent_records():
    """Multiple threads recording simultaneously should not lose entries."""
    errors = []

    def _writer(thread_id: int):
        try:
            for i in range(50):
                record(f"thread_{thread_id}_op_{i}", category="system")
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=_writer, args=(t,)) for t in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0
    entries = get_entries(limit=1000)
    assert len(entries) == 200  # 4 threads × 50 ops


# ── Serialization ────────────────────────────────────────────────────


def test_audit_entry_serialization():
    entry = record(
        "export",
        category="export",
        detail={"format": "parquet", "rows": 1000},
        duration_ms=99.9,
    )

    data = entry.model_dump()
    assert isinstance(data, dict)
    assert data["operation"] == "export"
    assert data["detail"]["rows"] == 1000

    # Round-trip
    restored = AuditEntry(**data)
    assert restored.id == entry.id
    assert restored.operation == entry.operation