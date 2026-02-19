# cloud/scheduler.py
"""
Background task scheduler for LIZARD Cloud operations.

Provides periodic execution of:
  - Health checks (connection liveness)
  - Stale connection detection
  - Audit log cleanup (trim old entries)
  - Config sync (re-read YAML)

Uses a lightweight thread-based approach — no external deps like Celery.
Each job is a simple async or sync callable with a configurable interval.
"""
from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional
from cloud.config import get_config
import structlog

log = structlog.get_logger(__name__)


# ── Job definition ───────────────────────────────────────────────────


class ScheduledJob:
    """A single scheduled job."""

    def __init__(
        self,
        name: str,
        func: Callable,
        interval_seconds: int,
        *,
        category: str = "system",
        enabled: bool = True,
        is_async: bool = False,
    ):
        self.name = name
        self.func = func
        self.interval_seconds = interval_seconds
        self.category = category
        self.enabled = enabled
        self.is_async = is_async

        # Runtime state
        self.last_run: Optional[str] = None
        self.last_status: Optional[str] = None
        self.last_duration_ms: Optional[float] = None
        self.last_error: Optional[str] = None
        self.run_count: int = 0
        self.error_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "interval_seconds": self.interval_seconds,
            "category": self.category,
            "enabled": self.enabled,
            "is_async": self.is_async,
            "last_run": self.last_run,
            "last_status": self.last_status,
            "last_duration_ms": self.last_duration_ms,
            "last_error": self.last_error,
            "run_count": self.run_count,
            "error_count": self.error_count,
        }


# ── Scheduler ────────────────────────────────────────────────────────


class Scheduler:
    """
    Simple background task scheduler.

    Call ``start()`` to begin the background loop and ``stop()`` to shut down.
    Jobs can be added/removed at any time.
    """

    def __init__(self):
        self._jobs: Dict[str, ScheduledJob] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ── Job management ────────────────────────────────────────────

    def add_job(self, job: ScheduledJob) -> None:
        with self._lock:
            self._jobs[job.name] = job
        log.info("scheduler_job_added", job=job.name, interval=job.interval_seconds)

    def remove_job(self, name: str) -> bool:
        with self._lock:
            if name in self._jobs:
                del self._jobs[name]
                log.info("scheduler_job_removed", job=name)
                return True
        return False

    def get_job(self, name: str) -> Optional[ScheduledJob]:
        with self._lock:
            return self._jobs.get(name)

    def list_jobs(self) -> List[ScheduledJob]:
        with self._lock:
            return list(self._jobs.values())

    def enable_job(self, name: str) -> bool:
        with self._lock:
            job = self._jobs.get(name)
            if job:
                job.enabled = True
                return True
        return False

    def disable_job(self, name: str) -> bool:
        with self._lock:
            job = self._jobs.get(name)
            if job:
                job.enabled = False
                return True
        return False

    # ── Lifecycle ─────────────────────────────────────────────────

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        log.info("scheduler_started")

    def stop(self) -> None:
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._thread = None
        log.info("scheduler_stopped")

    def _run_loop(self) -> None:
        """Main scheduler loop (runs in a background thread)."""
        # Create a dedicated event loop for async jobs
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        # Track last-run times
        last_runs: Dict[str, float] = {}

        try:
            while self._running:
                now = time.monotonic()

                with self._lock:
                    jobs_snapshot = list(self._jobs.values())

                for job in jobs_snapshot:
                    if not job.enabled:
                        continue

                    last = last_runs.get(job.name, 0)
                    if (now - last) < job.interval_seconds:
                        continue

                    last_runs[job.name] = now
                    self._execute_job(job)

                # Sleep 1 second between checks
                time.sleep(1)
        finally:
            self._loop.close()
            self._loop = None

    def _execute_job(self, job: ScheduledJob) -> None:
        """Execute a single job and update its state."""
        t0 = time.perf_counter()
        try:
            if job.is_async and self._loop:
                self._loop.run_until_complete(job.func())
            else:
                job.func()

            elapsed = (time.perf_counter() - t0) * 1000
            job.last_run = datetime.now(timezone.utc).isoformat()
            job.last_status = "ok"
            job.last_duration_ms = round(elapsed, 1)
            job.last_error = None
            job.run_count += 1

            log.debug(
                "scheduler_job_ok",
                job=job.name,
                ms=job.last_duration_ms,
            )

        except Exception as e:
            elapsed = (time.perf_counter() - t0) * 1000
            job.last_run = datetime.now(timezone.utc).isoformat()
            job.last_status = "error"
            job.last_duration_ms = round(elapsed, 1)
            job.last_error = str(e)
            job.run_count += 1
            job.error_count += 1

            log.warning(
                "scheduler_job_error",
                job=job.name,
                error=str(e),
            )

    # ── Run a job immediately (manual trigger) ────────────────────

    def run_now(self, name: str) -> Optional[Dict[str, Any]]:
        """Run a job immediately, return its status dict."""
        with self._lock:
            job = self._jobs.get(name)

        if not job:
            return None

        self._execute_job(job)
        return job.to_dict()


# ── Singleton ────────────────────────────────────────────────────────

_scheduler: Optional[Scheduler] = None
_scheduler_lock = threading.Lock()


def get_scheduler() -> Scheduler:
    """Return the global scheduler singleton."""
    global _scheduler
    if _scheduler is None:
        with _scheduler_lock:
            if _scheduler is None:
                _scheduler = Scheduler()
    return _scheduler


def reset_scheduler() -> None:
    """Stop and discard the global scheduler (for testing)."""
    global _scheduler
    with _scheduler_lock:
        if _scheduler is not None:
            _scheduler.stop()
            _scheduler = None


# ── Built-in job functions ───────────────────────────────────────────


def _job_health_check() -> None:
    """Periodic health check — logs connection status."""
    cfg = get_config()  # uses module-level import
    if not cfg.is_cloud_mode:
        return

    from cloud.audit import record

    record(
        "scheduled_health_check",
        category="system",
        detail={
            "databricks_connections": len(cfg.databricks_connections),
            "storage_connections": len(cfg.storage_connections),
        },
    )


def _job_config_sync() -> None:
    """Re-read config from YAML to pick up external changes."""
    get_config(reload=True)  # uses module-level import

    from cloud.audit import record

    record("scheduled_config_sync", category="config")

def _job_audit_trim() -> None:
    """Trim audit log if it exceeds threshold (no-op — ring buffer handles it)."""
    from cloud.audit import get_stats, record

    stats = get_stats()
    record(
        "scheduled_audit_trim",
        category="system",
        detail={"total_entries": stats["total_entries"]},
    )


def _job_stale_detection() -> None:
    """
    Detect stale connections — connections that haven't been tested recently.
    Logs a warning to the audit log.
    """
    from cloud.audit import record, get_entries

    cfg = get_config()  # uses module-level import
    if not cfg.is_cloud_mode:
        return

    # Check if any test_connection audit entries exist in the last 100 entries
    recent_tests = get_entries(limit=100, operation="test_connection")
    tested_names = {e.detail.get("connection_name") for e in recent_tests}

    all_names = set()
    for dc in cfg.databricks_connections:
        all_names.add(dc.name)
    for sc in cfg.storage_connections:
        all_names.add(sc.name)

    untested = all_names - tested_names

    if untested:
        record(
            "stale_connections_detected",
            category="connection",
            status="partial",
            detail={
                "untested_connections": sorted(untested),
                "count": len(untested),
            },
        )

# ── Default job registration ─────────────────────────────────────────


def register_default_jobs(scheduler: Optional[Scheduler] = None) -> Scheduler:
    """Register the built-in scheduled jobs."""
    s = scheduler or get_scheduler()

    s.add_job(ScheduledJob(
        name="health_check",
        func=_job_health_check,
        interval_seconds=300,  # 5 minutes
        category="system",
    ))

    s.add_job(ScheduledJob(
        name="config_sync",
        func=_job_config_sync,
        interval_seconds=600,  # 10 minutes
        category="config",
    ))

    s.add_job(ScheduledJob(
        name="audit_trim",
        func=_job_audit_trim,
        interval_seconds=3600,  # 1 hour
        category="system",
    ))

    s.add_job(ScheduledJob(
        name="stale_detection",
        func=_job_stale_detection,
        interval_seconds=1800,  # 30 minutes
        category="connection",
    ))

    return s