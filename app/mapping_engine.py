# app/mapping_engine.py
"""
Adapter: run the existing mapping suggestion logic on an in-memory DataFrame.

Imports the real functions from mapping.ai_mapper (where they are defined)
and from mapping.expr (for expression evaluation).
"""
from __future__ import annotations

import logging
from typing import Any, Dict

import pandas as pd

log = logging.getLogger("lizard.mapping_engine")


def suggest_mapping_for_dataframe(
    df: pd.DataFrame,
    engine: str = "heuristic",
) -> Dict[str, Any]:
    """
    Analyze a DataFrame and suggest field mapping + expressions,
    identical to what /mapping/templates/suggest returns for an uploaded file.
    """
    # These are the REAL functions from mapping/ai_mapper.py
    from mapping.ai_mapper import (
        suggest_event_mapping,
        suggest_mapping_with_scores,
        analyze_columns,
    )

    sample = df.head(100)

    # suggest_event_mapping returns {target: source, ..., "__expr__": {...}}
    mapping = suggest_event_mapping(sample, engine=engine)
    expressions = mapping.pop("__expr__", {})

    # suggest_mapping_with_scores returns {target: [{column, score}, ...]}
    candidates = suggest_mapping_with_scores(sample)

    # analyze_columns returns {col: {detected_type, null_count, ...}}
    column_analysis = analyze_columns(sample)

    return {
        "suggested_mapping": mapping,
        "suggested_expressions": expressions,
        "candidates": candidates,
        "column_analysis": column_analysis,
    }