# cloud/execution/local_engine.py
"""
Local pandas execution engine.

This wraps the existing PipelineExecutor from app/workbench_api.py so that
when mode == 'local', all behavior is *exactly* the same as before Phase 3.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List

import structlog

from cloud.execution.base import ExecutionEngine, ExecutionResult

log = structlog.get_logger(__name__)


class LocalPandasEngine(ExecutionEngine):
    """
    Executes pipelines locally using pandas via the existing PipelineExecutor.

    This is a thin wrapper — zero behavior change from existing workbench.
    """

    def __init__(self) -> None:
        # Lazy import to avoid circular deps at module level.
        # PipelineExecutor and get_source_data live in app.workbench_api.
        self._executor = None

    def _get_executor(self):
        """Lazy-load the existing PipelineExecutor."""
        if self._executor is None:
            from app.workbench_api import PipelineExecutor, get_source_data

            self._executor = PipelineExecutor(get_source_data)
        return self._executor

    # ── interface ────────────────────────────────────────────

    def engine_name(self) -> str:
        return "local"

    async def execute_pipeline(
        self,
        pipeline: List[Dict[str, Any]],
        *,
        limit: int = 1000,
        offset: int = 0,
    ) -> ExecutionResult:
        from app.workbench_api import PipelineStep

        steps = [PipelineStep(**s) for s in pipeline]
        executor = self._get_executor()

        t0 = time.perf_counter()
        df, total_rows = await executor.execute(steps, limit=limit, offset=offset)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        log.info(
            "local_pipeline_executed",
            rows=len(df),
            total=total_rows,
            ms=round(elapsed_ms, 1),
        )

        return ExecutionResult(
            data=df,
            total_rows=total_rows,
            columns=list(df.columns),
            engine="local",
            execution_time_ms=round(elapsed_ms, 1),
        )

    async def health_check(self) -> Dict[str, Any]:
        return {"engine": "local", "status": "ok"}