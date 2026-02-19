# app/ingest_engine.py
"""
Adapter: apply mapping to a DataFrame and ingest into the local event store.

This mirrors the logic inside POST /upload/events in app/main.py:
1. Rename columns via the field mapping (inverse: target->source becomes source->target rename)
2. Apply inline __expr__ expressions
3. Call ingest_events_dataframe to write into DuckDB/SQLAlchemy
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

import pandas as pd

log = logging.getLogger("lizard.ingest_engine")


def apply_mapping_and_ingest(
    df: pd.DataFrame,
    field_mapping: Dict[str, str],
    expressions: Dict[str, Any],
    source_name: str,
) -> Dict[str, Any]:
    """
    Apply field mapping + expressions to a DataFrame, then ingest rows
    into the local event store via ingest_events_dataframe.

    Returns dict with 'ingested' and 'rejected' counts.
    """
    # Lazy imports to avoid circular dependency at module level
    from app.main import ingest_events_dataframe, evaluate_mapping_expression
    from mapping.expr import evaluate_dataframe

    total_rows = len(df)
    work = df.copy()

    # --- Step 1: Apply column renames ---
    # field_mapping is {target_field: source_column}
    # We need the inverse for pd.rename: {source_column: target_field}
    str_map = {k: v for k, v in field_mapping.items() if isinstance(v, str)}
    cols = set(work.columns)
    # Drop mappings that reference columns not present in the DataFrame
    str_map = {k: v for k, v in str_map.items() if v in cols}
    # Inverse: {source -> target}
    inverse = {src: tgt for tgt, src in str_map.items()}
    work = work.rename(columns=inverse)

    # --- Step 2: Apply inline expressions ---
    if expressions:
        for tgt, expr_val in expressions.items():
            try:
                if isinstance(expr_val, dict):
                    work[tgt] = evaluate_mapping_expression(expr_val, work)
                elif isinstance(expr_val, str):
                    # Simple string expressions like "try_float", "upper", "bool"
                    work[tgt] = evaluate_mapping_expression(
                        {"op": expr_val, "col": tgt}, work
                    )
            except Exception as e:
                log.warning("expression_eval_failed", target=tgt, error=str(e))

    # --- Step 3: Set source column ---
    work["source"] = source_name

    # --- Step 4: Ingest via the existing async function ---
    try:
        # ingest_events_dataframe is async, so we need to run it
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # We're already inside an async context (FastAPI), use create_task
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                ingested = loop.run_in_executor(
                    pool,
                    lambda: asyncio.run(_async_ingest(work, source_name))
                )
                # Since we're in an async handler calling us, we handle this below
                # Actually this path shouldn't hit — see the async wrapper instead
                raise RuntimeError("Use async_apply_mapping_and_ingest instead")
        else:
            ingested = asyncio.run(_async_ingest(work, source_name))
    except RuntimeError:
        # If we can't determine the loop state, fall back
        ingested = asyncio.run(_async_ingest(work, source_name))

    return {
        "ingested": ingested,
        "rejected": total_rows - ingested,
    }


async def _async_ingest(work: pd.DataFrame, source_name: str) -> int:
    """Thin async wrapper around ingest_events_dataframe."""
    from app.main import ingest_events_dataframe
    return await ingest_events_dataframe(work, source_name=source_name)


async def async_apply_mapping_and_ingest(
    df: pd.DataFrame,
    field_mapping: Dict[str, str],
    expressions: Dict[str, Any],
    source_name: str,
) -> Dict[str, Any]:
    """
    Async version — call this directly from FastAPI async endpoint handlers.
    """
    from app.main import ingest_events_dataframe, evaluate_mapping_expression

    total_rows = len(df)
    work = df.copy()

    # Step 1: Column renames
    str_map = {k: v for k, v in field_mapping.items() if isinstance(v, str)}
    cols = set(work.columns)
    str_map = {k: v for k, v in str_map.items() if v in cols}
    inverse = {src: tgt for tgt, src in str_map.items()}
    work = work.rename(columns=inverse)

    # Step 2: Expressions
    if expressions:
        for tgt, expr_val in expressions.items():
            try:
                if isinstance(expr_val, dict):
                    work[tgt] = evaluate_mapping_expression(expr_val, work)
                elif isinstance(expr_val, str):
                    work[tgt] = evaluate_mapping_expression(
                        {"op": expr_val, "col": tgt}, work
                    )
            except Exception as e:
                log.warning("expression_eval_failed", target=tgt, error=str(e))

    # Step 3: Source
    work["source"] = source_name

    # Step 4: Ingest (properly awaited)
    ingested = await ingest_events_dataframe(work, source_name=source_name)

    return {
        "ingested": ingested,
        "rejected": total_rows - ingested,
    }