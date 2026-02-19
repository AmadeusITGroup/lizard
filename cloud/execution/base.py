# cloud/execution/base.py
"""
Abstract base class for execution engines.

An ExecutionEngine takes a pipeline definition (list of steps) and executes
it against a data source. In local mode this is pandas in-process. In cloud
mode this is Spark on Databricks.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class ExecutionResult:
    """Result returned by any ExecutionEngine.execute() call."""

    data: pd.DataFrame
    """The result DataFrame (always pandas — Spark results are collected back)."""

    total_rows: int
    """Total row count before pagination (limit/offset)."""

    columns: List[str]
    """Column names in order."""

    engine: str
    """Which engine produced this result ('local' or 'spark')."""

    execution_time_ms: float = 0.0
    """Wall-clock execution time in milliseconds."""

    cluster_id: Optional[str] = None
    """Databricks cluster ID if executed on Spark, else None."""

    warnings: List[str] = field(default_factory=list)
    """Any non-fatal warnings emitted during execution."""

    metadata: Dict[str, Any] = field(default_factory=dict)
    """Arbitrary metadata (Spark plan info, job run ID, etc.)."""

    def to_api_dict(self, limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
        """Serialize for the /workbench/query API response shape."""
        import numpy as np
        from datetime import datetime

        df = self.data
        records = df.replace({np.nan: None}).to_dict(orient="records")

        # Convert datetime/numpy types to JSON-serializable values
        for record in records:
            for key, value in record.items():
                if isinstance(value, (datetime, pd.Timestamp)):
                    record[key] = value.isoformat()
                elif hasattr(value, "item"):  # numpy scalar
                    record[key] = value.item()

        return {
            "data": records,
            "columns": self.columns,
            "row_count": len(records),
            "total_rows": self.total_rows,
            "limit": limit,
            "offset": offset,
            "engine": self.engine,
            "execution_time_ms": self.execution_time_ms,
            "cluster_id": self.cluster_id,
            "warnings": self.warnings,
        }


class ExecutionEngine(ABC):
    """
    Abstract execution engine.

    Every engine must be able to:
    1. Execute a pipeline against data sources and return an ExecutionResult
    2. Report what engine type it is
    3. Report whether it is currently available / healthy
    """

    @abstractmethod
    def engine_name(self) -> str:
        """Return the engine identifier: 'local' or 'spark'."""
        ...

    @abstractmethod
    async def execute_pipeline(
        self,
        pipeline: List[Dict[str, Any]],
        *,
        limit: int = 1000,
        offset: int = 0,
    ) -> ExecutionResult:
        """
        Execute a list of pipeline steps and return results.

        Each step is a dict with keys: {"type": str, "config": dict}
        (same schema as workbench_api.PipelineStep).
        """
        ...

    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """
        Check whether this engine is available.

        Returns a dict like:
          {"engine": "local", "status": "ok"}
          {"engine": "spark", "status": "ok", "cluster_id": "...", "cluster_state": "RUNNING"}
          {"engine": "spark", "status": "error", "error": "Cluster not running"}
        """
        ...

    async def preview_pipeline(
        self,
        pipeline: List[Dict[str, Any]],
        *,
        limit: int = 100,
    ) -> ExecutionResult:
        """
        Preview a pipeline with a small limit.

        Default implementation delegates to execute_pipeline.
        Engines can override for optimized preview behavior.
        """
        return await self.execute_pipeline(pipeline, limit=limit, offset=0)