# tests/test_scheduler.py
"""
Tests for cloud/scheduler.py.

Covers:
  - ScheduledJob model
  - Scheduler: add/remove/list/enable/disable jobs
  - Scheduler: start/stop lifecycle
  - Scheduler: job execution with timing and error capture
  - Scheduler: run_now (manual trigger)
  - Singleton: get_scheduler / reset_scheduler
  - Built-in job functions
  - register_default_jobs
"""
from __future__ import annotations

import time
import threading
from unittest.mock import patch, MagicMock

import pytest

from cloud.scheduler import (
    Scheduler,
    ScheduledJob,
    get_scheduler,
    reset_scheduler,
    register_default_jobs,
    _job_health_check,
    _job_config_sync,
    _job_audit_trim,
    _job_stale_detection,
)


# ── Fixtures ─────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _reset():
    """Ensure clean scheduler state for every test."""
    reset_scheduler()
    yield
    reset_scheduler()


# ── ScheduledJob model ───────────────────────────────────────────────


def test_scheduled_job_defaults():
    job = ScheduledJob(name="test", func=lambda: None, interval_seconds=60)
    assert job.name == "test"
    assert job.interval_seconds == 60
    assert job.enabled is True
    assert job.is_async is False
    assert job.run_count == 0
    assert job.error_count == 0
    assert job.last_run is None


def test_scheduled_job_to_dict():
    job = ScheduledJob(
        name="my_job",
        func=lambda: None,
        interval_seconds=120,
        category="config",
        enabled=False,
    )
    d = job.to_dict()
    assert d["name"] == "my_job"
    assert d["interval_seconds"] == 120
    assert d["category"] == "config"
    assert d["enabled"] is False
    assert d["run_count"] == 0


# ── Scheduler: job management ────────────────────────────────────────


def test_add_and_list_jobs():
    s = Scheduler()
    s.add_job(ScheduledJob(name="j1", func=lambda: None, interval_seconds=10))
    s.add_job(ScheduledJob(name="j2", func=lambda: None, interval_seconds=20))

    jobs = s.list_jobs()
    assert len(jobs) == 2
    assert {j.name for j in jobs} == {"j1", "j2"}


def test_remove_job():
    s = Scheduler()
    s.add_job(ScheduledJob(name="j1", func=lambda: None, interval_seconds=10))
    assert s.remove_job("j1") is True
    assert s.remove_job("j1") is False  # Already removed
    assert len(s.list_jobs()) == 0


def test_get_job():
    s = Scheduler()
    s.add_job(ScheduledJob(name="j1", func=lambda: None, interval_seconds=10))
    assert s.get_job("j1") is not None
    assert s.get_job("nonexistent") is None


def test_enable_disable_job():
    s = Scheduler()
    s.add_job(ScheduledJob(name="j1", func=lambda: None, interval_seconds=10))

    assert s.disable_job("j1") is True
    assert s.get_job("j1").enabled is False

    assert s.enable_job("j1") is True
    assert s.get_job("j1").enabled is True

    assert s.enable_job("nonexistent") is False
    assert s.disable_job("nonexistent") is False


# ── Scheduler: execution ─────────────────────────────────────────────


def test_execute_job_success():
    counter = {"count": 0}

    def increment():
        counter["count"] += 1

    s = Scheduler()
    job = ScheduledJob(name="inc", func=increment, interval_seconds=1)
    s.add_job(job)
    s._loop = None  # Not needed for sync

    s._execute_job(job)

    assert counter["count"] == 1
    assert job.run_count == 1
    assert job.error_count == 0
    assert job.last_status == "ok"
    assert job.last_run is not None
    assert job.last_duration_ms is not None
    assert job.last_error is None


def test_execute_job_error():
    def failing():
        raise RuntimeError("boom")

    s = Scheduler()
    job = ScheduledJob(name="fail", func=failing, interval_seconds=1)
    s.add_job(job)

    s._execute_job(job)

    assert job.run_count == 1
    assert job.error_count == 1
    assert job.last_status == "error"
    assert job.last_error == "boom"


def test_run_now():
    results = []

    def collect():
        results.append(True)

    s = Scheduler()
    s.add_job(ScheduledJob(name="collect", func=collect, interval_seconds=999))

    result = s.run_now("collect")
    assert result is not None
    assert result["last_status"] == "ok"
    assert len(results) == 1


def test_run_now_nonexistent():
    s = Scheduler()
    assert s.run_now("nonexistent") is None


# ── Scheduler: lifecycle ─────────────────────────────────────────────


def test_start_stop():
    s = Scheduler()
    assert s.is_running is False

    s.start()
    assert s.is_running is True

    # Starting again is a no-op
    s.start()
    assert s.is_running is True

    s.stop()
    assert s.is_running is False


def test_scheduler_executes_jobs_in_background():
    """Verify that the background loop actually runs jobs."""
    counter = {"count": 0}
    lock = threading.Lock()

    def increment():
        with lock:
            counter["count"] += 1

    s = Scheduler()
    s.add_job(ScheduledJob(name="bg", func=increment, interval_seconds=1))
    s.start()

    # Wait for at least one execution
    time.sleep(2.5)
    s.stop()

    with lock:
        assert counter["count"] >= 1


def test_disabled_job_not_executed():
    counter = {"count": 0}

    def increment():
        counter["count"] += 1

    s = Scheduler()
    s.add_job(ScheduledJob(
        name="disabled_job",
        func=increment,
        interval_seconds=1,
        enabled=False,
    ))
    s.start()
    time.sleep(2.5)
    s.stop()

    assert counter["count"] == 0


# ── Singleton ────────────────────────────────────────────────────────


def test_get_scheduler_singleton():
    s1 = get_scheduler()
    s2 = get_scheduler()
    assert s1 is s2


def test_reset_scheduler():
    s1 = get_scheduler()
    reset_scheduler()
    s2 = get_scheduler()
    assert s1 is not s2


# ── Built-in job functions ───────────────────────────────────────────


def test_job_health_check_local_mode():
    """Health check is a no-op in local mode."""
    from cloud.config import LizardCloudConfig

    with patch("cloud.scheduler.get_config") as mock:
        mock.return_value = LizardCloudConfig(mode="local")
        _job_health_check()  # Should not raise


def test_job_health_check_cloud_mode():
    from cloud.config import LizardCloudConfig
    from cloud import audit

    audit.clear()

    with patch("cloud.scheduler.get_config") as mock:
        mock.return_value = LizardCloudConfig(mode="cloud")
        _job_health_check()

    entries = audit.get_entries(operation="scheduled_health_check")
    assert len(entries) == 1

    audit.clear()


def test_job_config_sync():
    from cloud import audit

    audit.clear()

    with patch("cloud.scheduler.get_config") as mock:
        from cloud.config import LizardCloudConfig
        mock.return_value = LizardCloudConfig()
        _job_config_sync()

    entries = audit.get_entries(operation="scheduled_config_sync")
    assert len(entries) == 1

    audit.clear()


def test_job_audit_trim():
    from cloud import audit

    audit.clear()
    _job_audit_trim()

    entries = audit.get_entries(operation="scheduled_audit_trim")
    assert len(entries) == 1

    audit.clear()


def test_job_stale_detection_local_mode():
    """Stale detection is a no-op in local mode."""
    from cloud.config import LizardCloudConfig

    with patch("cloud.scheduler.get_config") as mock:
        mock.return_value = LizardCloudConfig(mode="local")
        _job_stale_detection()  # Should not raise


def test_job_stale_detection_finds_untested():
    from cloud.config import (
        LizardCloudConfig,
        DatabricksConnectionConfig,
        StorageConnectionConfig,
    )
    from cloud import audit

    audit.clear()

    cfg = LizardCloudConfig(
        mode="cloud",
        databricks_connections=[
            DatabricksConnectionConfig(name="ws1", workspace_id="id1"),
        ],
        storage_connections=[
            StorageConnectionConfig(name="st1", account_name="acc1"),
        ],
    )

    with patch("cloud.scheduler.get_config", return_value=cfg):
        _job_stale_detection()

    entries = audit.get_entries(operation="stale_connections_detected")
    assert len(entries) == 1
    assert set(entries[0].detail["untested_connections"]) == {"st1", "ws1"}

    audit.clear()


# ── register_default_jobs ────────────────────────────────────────────


def test_register_default_jobs():
    s = Scheduler()
    register_default_jobs(s)

    jobs = s.list_jobs()
    job_names = {j.name for j in jobs}
    assert "health_check" in job_names
    assert "config_sync" in job_names
    assert "audit_trim" in job_names
    assert "stale_detection" in job_names


def test_register_default_jobs_intervals():
    s = Scheduler()
    register_default_jobs(s)

    hc = s.get_job("health_check")
    assert hc.interval_seconds == 300

    cs = s.get_job("config_sync")
    assert cs.interval_seconds == 600

    at = s.get_job("audit_trim")
    assert at.interval_seconds == 3600

    sd = s.get_job("stale_detection")
    assert sd.interval_seconds == 1800