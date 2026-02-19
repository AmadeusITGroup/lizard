# path: app/workbench_api.py
"""
Data Workbench API - Advanced data manipulation, transformation, and view management.
Supports querying, joining, aggregating, and persisting derived datasets.
Works with ALL fields including custom fields and meta/raw_json fields.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import uuid
import json
import re
import os

from fastapi import APIRouter, HTTPException, Query, Body, Depends
from pydantic import BaseModel, Field
import pandas as pd
import numpy as np
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

# Import from domain/models.py where Event and DB utilities are defined
from domain.models import Event, Base, make_engine_and_session

# Execution engine (Phase 3) — lazy import to avoid circular deps at startup
_USE_ENGINE = True  # Feature flag — set False to disable engine layer entirely

# Import custom fields registry from mapping_api
try:
    from app.mapping_api import _custom_fields, get_all_target_fields
except ImportError:
    _custom_fields = {}


    def get_all_target_fields():
        return []

router = APIRouter(prefix="/workbench", tags=["Data Workbench"])

# ============================================================
# Database Session Setup (same pattern as main.py)
# ============================================================
DB_URL = os.getenv("LIZARD_DB_URL", "sqlite+aiosqlite:///./lizard.db")
_engine, _SessionLocal = make_engine_and_session(DB_URL)


async def get_workbench_session() -> AsyncSession:
    """Yield a database session."""
    async with _SessionLocal() as session:
        yield session


# ============================================================
# In-memory storage (replace with DB in production)
# ============================================================
_derived_views: Dict[str, Dict[str, Any]] = {}
_materialized_data: Dict[str, pd.DataFrame] = {}

# ============================================================
# Standard Event Fields (from domain/models.py Event class)
# ============================================================
STANDARD_EVENT_FIELDS = [
    "id", "ts", "source", "event_type", "user_id", "account_id",
    "device_id", "card_hash", "ip", "geo_lat", "geo_lon",
    "country", "city", "is_unusual", "meta",
    # NEW: Ticket fields
    "office_id", "user_sign", "organization", "pnr", "carrier",
    "origin", "dest", "tkt_number", "status", "pos_country",
    "card_country", "advance_hours", "stay_nights", "amount",
    "currency", "fop_type", "fop_name", "fop_subtype",
    "card_last4", "card_bin", "is_fraud_indicator", "failure_reason", "legs"
]


# ============================================================
# Helper:  Convert Event to Dict with ALL fields (including meta)
# ============================================================
def event_to_full_dict(event: Event) -> Dict[str, Any]:
    """
    Convert an Event to a dictionary including ALL fields.
    Extracts fields from the 'meta' JSON column and flattens them.
    """
    record = {
        "id": event.id,
        "ts": event.ts,
        "source": event.source,
        "event_type": event.event_type,
        "user_id": event.user_id,
        "account_id": event.account_id,
        "device_id": event.device_id,
        "ip": event.ip,
        "geo_lat": event.geo_lat,
        "geo_lon": event.geo_lon,
        "country": event.country,
        "city": event.city,
        "card_hash": event.card_hash,
        "is_unusual": event.is_unusual,

        # NEW: Ticket fields
        "office_id": event.office_id,
        "user_sign": event.user_sign,
        "organization": event.organization,
        "pnr": event.pnr,
        "carrier": event.carrier,
        "origin": event.origin,
        "dest": event.dest,
        "tkt_number": event.tkt_number,
        "status": event.status,
        "pos_country": event.pos_country,
        "card_country": event.card_country,
        "advance_hours": event.advance_hours,
        "stay_nights": event.stay_nights,
        "amount": event.amount,
        "currency": event.currency,
        "fop_type": event.fop_type,
        "fop_name": event.fop_name,
        "fop_subtype": event.fop_subtype,
        "card_last4": event.card_last4,
        "card_bin": event.card_bin,
        "is_fraud_indicator": event.is_fraud_indicator,
        "failure_reason": event.failure_reason,
        "legs": event.legs,
    }

    # Flatten meta fields into the record (for truly dynamic fields)
    if event.meta:
        for key, value in event.meta.items():
            # Don't override standard fields
            if key not in record:
                record[key] = value

    return record

# ============================================================
# Helper: Get all available fields for a source (dynamic discovery)
# ============================================================
async def discover_source_fields(source_name: str) -> Dict[str, Dict[str, Any]]:
    """
    Dynamically discover all fields available in a data source.
    Includes standard fields + all fields from meta JSON.
    """
    columns = {}

    async with _SessionLocal() as session:
        # Get sample events to discover fields
        sample_query = select(Event).where(Event.source == source_name).limit(2000)
        result = await session.execute(sample_query)
        samples = result.scalars().all()

        if not samples:
            return columns

        # Collect all field names and sample values
        field_values: Dict[str, List[Any]] = {}

        for event in samples:
            record = event_to_full_dict(event)
            for field, value in record.items():
                if field not in field_values:
                    field_values[field] = []
                if value is not None:
                    field_values[field].append(value)

        # Analyze each field
        for field, values in field_values.items():
            if not values:
                columns[field] = {
                    "type": "unknown",
                    "null_percent": 100.0,
                "sample":  None,
                }
                continue

            # Detect type from values
            sample_val = values[0]
            if isinstance(sample_val, bool):
                field_type = "boolean"
            elif isinstance(sample_val, int):
                field_type = "integer"
            elif isinstance(sample_val, float):
                field_type = "number"
            elif isinstance(sample_val, datetime):
                field_type = "datetime"
            elif isinstance(sample_val, (list, dict)):
                field_type = "json"
            else:
                field_type = "string"

            # Calculate stats
            unique_values = set(str(v) for v in values[:100])
            null_percent = ((len(samples) - len(values)) / len(samples)) * 100

            columns[field] = {
                "type": field_type,
                "null_percent": round(null_percent, 1),
                "unique_count": len(unique_values),
                "sample": str(sample_val)[:100] if sample_val else None,
                "samples": [str(v)[:50] for v in values[:5]],
                "is_standard": field in STANDARD_EVENT_FIELDS,
                "is_custom": field in _custom_fields,
            }

    return columns


# ============================================================
# Pydantic Models
# ============================================================

class PipelineStep(BaseModel):
    type: str  # 'source', 'filter', 'select', 'join', 'aggregate', 'transform', 'sort', 'limit', 'union'
    config: Dict[str, Any] = Field(default_factory=dict)


class QueryRequest(BaseModel):
    pipeline: List[PipelineStep]
    limit: int = 1000
    offset: int = 0


class PreviewRequest(BaseModel):
    pipeline: List[PipelineStep]
    limit: int = 100


class DerivedViewCreate(BaseModel):
    name: str
    description: str = ""
    pipeline: List[PipelineStep]
    is_materialized: bool = False
    tags: List[str] = Field(default_factory=list)


class DerivedViewUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    pipeline: Optional[List[PipelineStep]] = None
    tags: Optional[List[str]] = None


class ExportRequest(BaseModel):
    pipeline: List[PipelineStep]
    format: str = "csv"  # csv, json, parquet
    limit: Optional[int] = None


# ============================================================
# Data Source Provider (DYNAMIC - supports all fields)
# ============================================================

async def get_source_data(table_name: str) -> pd.DataFrame:
    """
    Load data from the database including ALL fields.
    Flattens meta JSON fields into columns.
    """
    # Check if it's a materialized view
    if table_name in _materialized_data:
        return _materialized_data[table_name].copy()

    # Check if it's a virtual derived view
    for view_id, view in _derived_views.items():
        if view["name"] == table_name and not view.get("is_materialized"):
            # Execute the view's pipeline
            view_pipeline = [PipelineStep(**s) for s in view["pipeline"]]
            executor = PipelineExecutor(get_source_data)
            df, _ = await executor.execute(view_pipeline, limit=100000, offset=0)
            return df

    # Load from events table
    async with _SessionLocal() as session:
        query = select(Event).where(Event.source == table_name).limit(100000)
        result = await session.execute(query)
        events = result.scalars().all()

        if events:
            # Convert all events to dicts with flattened meta fields
            records = [event_to_full_dict(event) for event in events]
            return pd.DataFrame(records)

        return pd.DataFrame()


# ============================================================
# Pipeline Execution Engine
# ============================================================

class PipelineExecutor:
    """Executes data transformation pipelines."""

    def __init__(self, get_source_data_func):
        self.get_source_data = get_source_data_func
        self._source_cache: Dict[str, pd.DataFrame] = {}

    async def execute(self, pipeline: List[PipelineStep], limit: int = 1000, offset: int = 0) -> tuple:
        """Execute a pipeline and return results."""
        if not pipeline:
            raise ValueError("Pipeline cannot be empty")

        df = None

        for step in pipeline:
            step_type = step.type
            config = step.config

            if step_type == "source":
                df = await self._execute_source(config)
            elif step_type == "filter":
                df = self._execute_filter(df, config)
            elif step_type == "select":
                df = self._execute_select(df, config)
            elif step_type == "join":
                df = await self._execute_join(df, config)
            elif step_type == "aggregate":
                df = self._execute_aggregate(df, config)
            elif step_type == "transform":
                df = self._execute_transform(df, config)
            elif step_type == "sort":
                df = self._execute_sort(df, config)
            elif step_type == "limit":
                df = self._execute_limit(df, config)
            elif step_type == "union":
                df = await self._execute_union(df, config)
            elif step_type == "distinct":
                df = self._execute_distinct(df, config)
            elif step_type == "rename":
                df = self._execute_rename(df, config)
            elif step_type == "drop":
                df = self._execute_drop(df, config)
            else:
                raise ValueError(f"Unknown step type: {step_type}")

        if df is None:
            raise ValueError("Pipeline must start with a 'source' step")

        # Apply final limit/offset
        total_rows = len(df)
        df = df.iloc[offset:offset + limit]

        return df, total_rows

    async def _execute_source(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Load data from a source table or view."""
        table_name = config.get("table")
        if not table_name:
            raise ValueError("Source step requires 'table' in config")

        # Check cache first
        if table_name in self._source_cache:
            return self._source_cache[table_name].copy()

        # Load data (this now includes ALL fields)
        df = await self.get_source_data(table_name)
        self._source_cache[table_name] = df
        return df.copy()

    def _execute_filter(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Apply filter conditions."""
        if df is None:
            raise ValueError("Filter step requires data from previous step")

        conditions = config.get("conditions", [])
        if not conditions and "field" in config:
            # Single condition shorthand
            conditions = [config]

        for cond in conditions:
            field = cond.get("field")
            op = cond.get("op", "eq")
            value = cond.get("value")

            if field not in df.columns:
                continue

            if op == "eq":
                df = df[df[field] == value]
            elif op == "ne":
                df = df[df[field] != value]
            elif op == "gt":
                df = df[df[field] > value]
            elif op == "gte":
                df = df[df[field] >= value]
            elif op == "lt":
                df = df[df[field] < value]
            elif op == "lte":
                df = df[df[field] <= value]
            elif op == "in":
                df = df[df[field].isin(value if isinstance(value, list) else [value])]
            elif op == "nin":
                df = df[~df[field].isin(value if isinstance(value, list) else [value])]
            elif op == "contains":
                df = df[df[field].astype(str).str.contains(str(value), case=False, na=False)]
            elif op == "startswith":
                df = df[df[field].astype(str).str.startswith(str(value), na=False)]
            elif op == "endswith":
                df = df[df[field].astype(str).str.endswith(str(value), na=False)]
            elif op == "isnull":
                df = df[df[field].isna()]
            elif op == "notnull":
                df = df[df[field].notna()]
            elif op == "regex":
                df = df[df[field].astype(str).str.match(str(value), na=False)]
            elif op == "between":
                if isinstance(value, list) and len(value) == 2:
                    df = df[(df[field] >= value[0]) & (df[field] <= value[1])]

        return df

    def _execute_select(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Select specific columns."""
        if df is None:
            raise ValueError("Select step requires data from previous step")

        columns = config.get("columns", [])
        if not columns:
            return df

        # Handle column aliases
        existing_cols = []
        rename_map = {}

        for col in columns:
            if isinstance(col, dict):
                src = col.get("source")
                alias = col.get("alias", src)
                if src in df.columns:
                    existing_cols.append(src)
                    if alias != src:
                        rename_map[src] = alias
            elif col in df.columns:
                existing_cols.append(col)

        df = df[existing_cols]
        if rename_map:
            df = df.rename(columns=rename_map)

        return df

    # In PipelineExecutor class, update _execute_join method:

    # In PipelineExecutor class, REPLACE the existing _execute_join method with this:

    async def _execute_join(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """
        Join with another table.

        ENHANCED: Now supports time-window joins for correlating events.

        Time window config example:
        {
            "table": "ticket_events",
            "type": "left",
            "on": [{"left": "user_id", "right": "user_id"}],
            "time_window": {
                "enabled": true,
                "left_col": "ts",
                "right_col":  "ts",
                "value": 1,
                "unit": "hours",
                "direction": "after"  # "after", "before", or "around"
            }
        }
        """
        if df is None:
            raise ValueError("Join step requires data from previous step")

        right_table = config.get("table")
        join_conditions = config.get("on", [])
        join_type = config.get("type", "left")
        time_window = config.get("time_window", {})

        # Skip if not fully configured yet
        if not right_table:
            return df  # Return unchanged data if no table selected

        # Load right table (includes all fields)
        right_df = await self._execute_source({"table": right_table})

        if right_df.empty:
            return df

        # Build join keys - filter out empty conditions
        left_on = []
        right_on = []

        for cond in join_conditions:
            left_col = cond.get("left", "").strip() if cond.get("left") else ""
            right_col = cond.get("right", "").strip() if cond.get("right") else ""
            if left_col and right_col:
                left_on.append(left_col)
                right_on.append(right_col)

        # Add prefix to avoid column conflicts
        right_prefix = config.get("right_prefix", f"{right_table}_")

        # Rename right table columns to avoid conflicts (except join keys)
        right_df_renamed = right_df.copy()
        rename_cols = {}
        for col in right_df.columns:
            if col in df.columns and col not in right_on:
                rename_cols[col] = f"{right_prefix}{col}"
        right_df_renamed = right_df_renamed.rename(columns=rename_cols)

        # Update right_on with renamed columns if needed
        right_on_renamed = [rename_cols.get(c, c) for c in right_on]

        # Map join types
        how_map = {"inner": "inner", "left": "left", "right": "right", "outer": "outer", "full": "outer"}
        how = how_map.get(join_type, "left")

        # ============================================================
        # TIME WINDOW JOIN LOGIC
        # ============================================================
        if time_window.get("enabled"):
            left_ts_col = time_window.get("left_col", "ts")
            right_ts_col = time_window.get("right_col", "ts")
            window_value = time_window.get("value", 1)
            window_unit = time_window.get("unit", "hours")
            direction = time_window.get("direction", "after")

            # Check if timestamp column was renamed
            right_ts_renamed = rename_cols.get(right_ts_col, right_ts_col)

            # Convert timestamp columns to datetime if needed
            if left_ts_col in df.columns:
                df = df.copy()
                df[left_ts_col] = pd.to_datetime(df[left_ts_col], errors='coerce')

            if right_ts_renamed in right_df_renamed.columns:
                right_df_renamed[right_ts_renamed] = pd.to_datetime(
                    right_df_renamed[right_ts_renamed], errors='coerce'
                )

            # Calculate time delta
            if window_unit == "minutes":
                delta = pd.Timedelta(minutes=window_value)
            elif window_unit == "hours":
                delta = pd.Timedelta(hours=window_value)
            elif window_unit == "days":
                delta = pd.Timedelta(days=window_value)
            else:
                delta = pd.Timedelta(hours=window_value)

            # For time window joins with equality conditions:
            # 1. First do the equality join
            # 2. Then filter by time window

            if left_on and right_on_renamed:
                # Merge on equality conditions first
                try:
                    merged = pd.merge(
                        df,
                        right_df_renamed,
                        left_on=left_on,
                        right_on=right_on_renamed,
                        how=how,
                        suffixes=('', '_dup')
                    )
                except Exception as e:
                    print(f"Merge with equality failed: {e}")
                    # Fall back to cross join approach
                    merged = self._cross_join_with_time_window(
                        df, right_df_renamed, left_ts_col, right_ts_renamed,
                        delta, direction, how
                    )
                    return merged
            else:
                # No equality conditions - do cross join filtered by time
                merged = self._cross_join_with_time_window(
                    df, right_df_renamed, left_ts_col, right_ts_renamed,
                    delta, direction, how
                )
                return merged

            # Apply time window filter on the merged result
            if left_ts_col in merged.columns and right_ts_renamed in merged.columns:
                if direction == "after":
                    # Right timestamp should be between left and left + delta
                    mask = (
                            (merged[right_ts_renamed] >= merged[left_ts_col]) &
                            (merged[right_ts_renamed] <= merged[left_ts_col] + delta)
                    )
                elif direction == "before":
                    # Right timestamp should be between left - delta and left
                    mask = (
                            (merged[right_ts_renamed] >= merged[left_ts_col] - delta) &
                            (merged[right_ts_renamed] <= merged[left_ts_col])
                    )
                else:  # "around"
                    # Right timestamp should be between left - delta and left + delta
                    mask = (
                            (merged[right_ts_renamed] >= merged[left_ts_col] - delta) &
                            (merged[right_ts_renamed] <= merged[left_ts_col] + delta)
                    )

                # For left/outer joins, keep rows where right side is NULL
                if how in ["left", "outer"]:
                    merged = merged[mask | merged[right_ts_renamed].isna()]
                else:
                    merged = merged[mask]

            return merged

        # ============================================================
        # STANDARD JOIN (no time window)
        # ============================================================
        if not left_on:
            return df  # Return unchanged if no valid conditions

        try:
            merged = pd.merge(
                df,
                right_df_renamed,
                left_on=left_on,
                right_on=right_on_renamed,
                how=how,
                suffixes=('', '_dup')
            )
        except Exception as e:
            # If merge fails, return original data
            print(f"Join failed: {e}")
            return df

        return merged

    def _cross_join_with_time_window(
            self,
            left_df: pd.DataFrame,
            right_df: pd.DataFrame,
            left_ts_col: str,
            right_ts_col: str,
            delta: pd.Timedelta,
            direction: str,
            how: str
    ) -> pd.DataFrame:
        """
        Perform a cross join filtered by time window.
        Used when there are no equality join conditions.

        WARNING: This can be expensive for large datasets!
        """
        # For very large datasets, limit the cross join
        max_left = 10000
        max_right = 10000

        if len(left_df) > max_left:
            print(f"Warning:  Limiting left table from {len(left_df)} to {max_left} rows for cross join")
            left_df = left_df.head(max_left)

        if len(right_df) > max_right:
            print(f"Warning: Limiting right table from {len(right_df)} to {max_right} rows for cross join")
            right_df = right_df.head(max_right)

        # Add temp key for cross join
        left_df = left_df.copy()
        right_df = right_df.copy()
        left_df["_tmp_key"] = 1
        right_df["_tmp_key"] = 1

        # Perform cross join
        merged = pd.merge(left_df, right_df, on="_tmp_key", how="inner", suffixes=('', '_right'))
        merged = merged.drop(columns=["_tmp_key"], errors="ignore")

        # Apply time window filter
        if left_ts_col in merged.columns and right_ts_col in merged.columns:
            if direction == "after":
                mask = (
                        (merged[right_ts_col] >= merged[left_ts_col]) &
                        (merged[right_ts_col] <= merged[left_ts_col] + delta)
                )
            elif direction == "before":
                mask = (
                        (merged[right_ts_col] >= merged[left_ts_col] - delta) &
                        (merged[right_ts_col] <= merged[left_ts_col])
                )
            else:  # "around"
                mask = (
                        (merged[right_ts_col] >= merged[left_ts_col] - delta) &
                        (merged[right_ts_col] <= merged[left_ts_col] + delta)
                )

            merged = merged[mask]

        return merged
    def _execute_aggregate(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Perform aggregation."""
        if df is None:
            raise ValueError("Aggregate step requires data from previous step")

        group_by = config.get("group_by", [])
        aggregations = config.get("aggregations", [])

        # If nothing configured, return as-is
        if not group_by and not aggregations:
            return df

        # Validate group_by columns exist
        valid_group_by = [g for g in group_by if g in df.columns]

        # Build aggregation specs - filter out incomplete aggregations
        agg_specs = {}

        for agg in aggregations:
            out_col = agg.get("column", "").strip()
            func_name = agg.get("func", "count")
            field = agg.get("field", "*")

            # Skip if output column name is empty
            if not out_col:
                continue

            if func_name == "count" and field == "*":
                # Count all rows - use first column
                if valid_group_by:
                    first_col = valid_group_by[0]
                    agg_specs[out_col] = (first_col, 'count')
                elif len(df.columns) > 0:
                    agg_specs[out_col] = (df.columns[0], 'count')
                continue

            if field not in df.columns and field != "*":
                continue

            func_map = {
                "count": "count",
                "sum": "sum",
                "avg": "mean",
                "mean": "mean",
                "min": "min",
                "max": "max",
                "first": "first",
                "last": "last",
                "std": "std",
                "var": "var",
                "median": "median",
                "count_distinct": "nunique",
                "nunique": "nunique",
            }

            pandas_func = func_map.get(func_name)
            if pandas_func:
                agg_specs[out_col] = (field, pandas_func)

        # Perform aggregation
        if valid_group_by and agg_specs:
            try:
                df = df.groupby(valid_group_by, as_index=False).agg(**agg_specs)
            except Exception as e:
                print(f"Aggregation failed:  {e}")
                # Fallback to simple groupby count
                df = df.groupby(valid_group_by, as_index=False).size()
                df = df.rename(columns={"size": "count"})
        elif valid_group_by:
            # Just group and count
            df = df.groupby(valid_group_by, as_index=False).size()
            df = df.rename(columns={"size": "count"})
        elif agg_specs:
            # Global aggregation (no group by)
            result = {}
            for out_col, (field, func_name) in agg_specs.items():
                try:
                    if func_name == 'count':
                        result[out_col] = len(df)
                    else:
                        result[out_col] = getattr(df[field], func_name)()
                except Exception:
                    result[out_col] = None
            df = pd.DataFrame([result])

        return df
    def _execute_transform(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Add computed columns."""
        if df is None:
            raise ValueError("Transform step requires data from previous step")

        transforms = config.get("transforms", [])
        if not transforms and "column" in config:
            transforms = [config]

        for transform in transforms:
            column = transform.get("column")
            expression = transform.get("expression", "")

            if not column or not expression:
                continue

            try:
                df[column] = self._evaluate_expression(df, expression)
            except Exception as e:
                # On error, set to None
                df[column] = None

        return df

    def _evaluate_expression(self, df: pd.DataFrame, expression: str) -> pd.Series:
        """Evaluate a transformation expression."""
        expr = expression.strip()

        # Direct column reference
        if expr in df.columns:
            return df[expr]

        # String literal
        if (expr.startswith('"') and expr.endswith('"')) or (expr.startswith("'") and expr.endswith("'")):
            return pd.Series([expr[1:-1]] * len(df), index=df.index)

        # Numeric literal
        try:
            num = float(expr)
            return pd.Series([num] * len(df), index=df.index)
        except ValueError:
            pass

        # Function expressions
        func_match = re.match(r'(\w+)\((.*)\)', expr)
        if func_match:
            func_name = func_match.group(1).lower()
            args = func_match.group(2)

            if func_name == "upper":
                col = args.strip()
                if col in df.columns:
                    return df[col].astype(str).str.upper()
            elif func_name == "lower":
                col = args.strip()
                if col in df.columns:
                    return df[col].astype(str).str.lower()
            elif func_name == "trim":
                col = args.strip()
                if col in df.columns:
                    return df[col].astype(str).str.strip()
            elif func_name == "length" or func_name == "len":
                col = args.strip()
                if col in df.columns:
                    return df[col].astype(str).str.len()
            elif func_name == "concat":
                parts = [p.strip() for p in args.split(",")]
                result = pd.Series([""] * len(df), index=df.index)
                for part in parts:
                    if part in df.columns:
                        result = result + df[part].astype(str).fillna("")
                    elif part.startswith('"') or part.startswith("'"):
                        result = result + part[1:-1]
                    else:
                        result = result + part
                return result
            elif func_name == "coalesce":
                parts = [p.strip() for p in args.split(",")]
                result = pd.Series([None] * len(df), index=df.index)
                for part in parts:
                    if part in df.columns:
                        result = result.fillna(df[part])
                    else:
                        result = result.fillna(part)
                return result
            elif func_name in ("hour", "extract_hour"):
                col = args.strip()
                if col in df.columns:
                    return pd.to_datetime(df[col], errors='coerce').dt.hour
            elif func_name in ("date", "extract_date"):
                col = args.strip()
                if col in df.columns:
                    return pd.to_datetime(df[col], errors='coerce').dt.date
            elif func_name in ("dayofweek", "dow"):
                col = args.strip()
                if col in df.columns:
                    return pd.to_datetime(df[col], errors='coerce').dt.dayofweek
            elif func_name == "year":
                col = args.strip()
                if col in df.columns:
                    return pd.to_datetime(df[col], errors='coerce').dt.year
            elif func_name == "month":
                col = args.strip()
                if col in df.columns:
                    return pd.to_datetime(df[col], errors='coerce').dt.month
            elif func_name == "day":
                col = args.strip()
                if col in df.columns:
                    return pd.to_datetime(df[col], errors='coerce').dt.day
            elif func_name == "round":
                parts = [p.strip() for p in args.split(",")]
                col = parts[0]
                decimals = int(parts[1]) if len(parts) > 1 else 0
                if col in df.columns:
                    return df[col].round(decimals)
            elif func_name == "abs":
                col = args.strip()
                if col in df.columns:
                    return df[col].abs()
            elif func_name == "floor":
                col = args.strip()
                if col in df.columns:
                    return np.floor(df[col])
            elif func_name == "ceil":
                col = args.strip()
                if col in df.columns:
                    return np.ceil(df[col])
            elif func_name == "isnull":
                col = args.strip()
                if col in df.columns:
                    return df[col].isna()
            elif func_name == "notnull":
                col = args.strip()
                if col in df.columns:
                    return df[col].notna()
            elif func_name == "ifnull":
                parts = [p.strip() for p in args.split(",")]
                if len(parts) >= 2:
                    col = parts[0]
                    default = parts[1]
                    if col in df.columns:
                        default_val = df[default] if default in df.columns else default
                        return df[col].fillna(default_val)

        # Arithmetic expressions:  col1 + col2, etc.
        for op in ['+', '-', '*', '/', '%']:
            if op in expr:
                parts = expr.split(op, 1)
                if len(parts) == 2:
                    left = parts[0].strip()
                    right = parts[1].strip()

                    # Get left value
                    if left in df.columns:
                        left_val = df[left]
                    else:
                        try:
                            left_val = float(left)
                        except ValueError:
                            left_val = left

                    # Get right value
                    if right in df.columns:
                        right_val = df[right]
                    else:
                        try:
                            right_val = float(right)
                        except ValueError:
                            right_val = right

                    if op == '+':
                        return left_val + right_val
                    elif op == '-':
                        return left_val - right_val
                    elif op == '*':
                        return left_val * right_val
                    elif op == '/':
                        return left_val / right_val
                    elif op == '%':
                        return left_val % right_val

        # Default:  return as string constant
        return pd.Series([expr] * len(df), index=df.index)

    def _execute_sort(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Sort data."""
        if df is None:
            raise ValueError("Sort step requires data from previous step")

        sort_by = config.get("by", [])
        if not sort_by and "field" in config:
            sort_by = [{"field": config["field"], "direction": config.get("direction", "asc")}]

        if not sort_by:
            return df

        columns = []
        ascending = []

        for s in sort_by:
            field = s.get("field")
            direction = s.get("direction", "asc")
            if field in df.columns:
                columns.append(field)
                ascending.append(direction.lower() == "asc")

        if columns:
            df = df.sort_values(by=columns, ascending=ascending)

        return df

    def _execute_limit(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Limit rows."""
        if df is None:
            raise ValueError("Limit step requires data from previous step")

        n = config.get("n", 1000)
        offset = config.get("offset", 0)

        return df.iloc[offset:offset + n]

    async def _execute_union(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Union with another table."""
        tables = config.get("tables", [])

        dfs = [df] if df is not None else []

        for table in tables:
            table_df = await self._execute_source({"table": table})
            dfs.append(table_df)

        if not dfs:
            raise ValueError("Union requires at least one table")

        return pd.concat(dfs, ignore_index=True)

    def _execute_distinct(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Remove duplicates."""
        if df is None:
            raise ValueError("Distinct step requires data from previous step")

        columns = config.get("columns", [])
        if columns:
            valid_cols = [c for c in columns if c in df.columns]
            if valid_cols:
                df = df.drop_duplicates(subset=valid_cols)
        else:
            df = df.drop_duplicates()

        return df

    def _execute_rename(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Rename columns."""
        if df is None:
            raise ValueError("Rename step requires data from previous step")

        mappings = config.get("mappings", {})
        if mappings:
            valid_mappings = {k: v for k, v in mappings.items() if k in df.columns}
            df = df.rename(columns=valid_mappings)

        return df

    def _execute_drop(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Drop columns."""
        if df is None:
            raise ValueError("Drop step requires data from previous step")

        columns = config.get("columns", [])
        if columns:
            df = df.drop(columns=[c for c in columns if c in df.columns], errors='ignore')

        return df


# Global executor instance
_executor = PipelineExecutor(get_source_data)


# ============================================================
# API Endpoints
# ============================================================

@router.get("/sources")
async def list_sources() -> List[Dict[str, Any]]:
    """List all available data sources (tables and views)."""
    sources = []

    # Get actual data sources from events table
    async with _SessionLocal() as session:
        query = select(
            Event.source,
            func.count(Event.id).label("row_count"),
            func.min(Event.ts).label("min_ts"),
            func.max(Event.ts).label("max_ts"),
        ).group_by(Event.source)

        result = await session.execute(query)
        rows = result.all()

        for row in rows:
            sources.append({
                "name": row.source,
                "type": "table",
                "row_count": row.row_count,
                "min_ts": row.min_ts.isoformat() if row.min_ts else None,
                "max_ts": row.max_ts.isoformat() if row.max_ts else None,
                "description": f"Imported data source: {row.source}",
            })

    # Add derived views
    for view_id, view in _derived_views.items():
        sources.append({
            "name": view["name"],
            "type": "view",
            "view_id": view_id,
            "is_materialized": view.get("is_materialized", False),
            "row_count": view.get("row_count"),
            "description": view.get("description", ""),
            "source_tables": view.get("source_tables", []),
            "created_at": view.get("created_at"),
        })

    return sources


@router.get("/sources/{source_name}")
async def get_source_info(source_name: str) -> Dict[str, Any]:
    """Get detailed information about a data source including ALL fields."""
    # Check if it's a derived view
    for view_id, view in _derived_views.items():
        if view["name"] == source_name:
            return {
                "name": source_name,
                "type": "view",
                "view_id": view_id,
                **view,
            }

    # Query the events table
    async with _SessionLocal() as session:
        # Get basic stats
        query = select(
            func.count(Event.id).label("row_count"),
            func.min(Event.ts).label("min_ts"),
            func.max(Event.ts).label("max_ts"),
        ).where(Event.source == source_name)

        result = await session.execute(query)
        row = result.first()

        if not row or row.row_count == 0:
            raise HTTPException(status_code=404, detail=f"Source '{source_name}' not found")

        # Discover all fields dynamically (including meta fields)
        columns = await discover_source_fields(source_name)

        return {
            "name": source_name,
            "type": "table",
            "row_count": row.row_count,
            "min_ts": row.min_ts.isoformat() if row.min_ts else None,
            "max_ts": row.max_ts.isoformat() if row.max_ts else None,
            "columns": columns,
        }


@router.get("/sources/{source_name}/columns")
async def get_source_columns(source_name: str) -> List[Dict[str, Any]]:
    """Get list of columns for a source with detailed info."""
    columns = await discover_source_fields(source_name)
    return [
        {"name": name, **info}
        for name, info in columns.items()
    ]


@router.post("/query")
async def execute_query(request: QueryRequest) -> Dict[str, Any]:
    """Execute a pipeline query."""
    try:
        # Phase 3: Try execution engine abstraction first
        if _USE_ENGINE:
            try:
                from cloud.execution.engine_factory import get_engine

                eng = get_engine()
                pipeline_dicts = [s.model_dump() for s in request.pipeline]
                result = await eng.execute_pipeline(
                    pipeline_dicts, limit=request.limit, offset=request.offset
                )
                return result.to_api_dict(limit=request.limit, offset=request.offset)
            except ImportError:
                pass  # cloud package not available — fall through to legacy

        # Legacy path (identical to pre-Phase-3 behavior)
        df, total_rows = await _executor.execute(
            [PipelineStep(**s.model_dump()) for s in request.pipeline],
            limit=request.limit,
            offset=request.offset
        )

        # Convert to records
        records = df.replace({np.nan: None}).to_dict(orient="records")

        # Convert datetime objects to strings
        for record in records:
            for key, value in record.items():
                if isinstance(value, (datetime, pd.Timestamp)):
                    record[key] = value.isoformat()
                elif hasattr(value, 'item'):  # numpy types
                    record[key] = value.item()

        return {
            "data": records,
            "columns": list(df.columns),
            "row_count": len(records),
            "total_rows": total_rows,
            "limit": request.limit,
            "offset": request.offset,
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/preview")
async def preview_query(request: PreviewRequest) -> Dict[str, Any]:
    """Preview a pipeline query with limited results and column stats."""
    try:
        # Phase 3: Try execution engine abstraction first
        if _USE_ENGINE:
            try:
                from cloud.execution.engine_factory import get_engine

                eng = get_engine()
                pipeline_dicts = [s.model_dump() for s in request.pipeline]
                result = await eng.preview_pipeline(pipeline_dicts, limit=request.limit)
                api_dict = result.to_api_dict(limit=request.limit, offset=0)

                # Add column stats (engine-agnostic)
                df = result.data
                column_stats = {}
                for col in df.columns:
                    stats = {
                        "type": str(df[col].dtype),
                        "null_count": int(df[col].isna().sum()),
                        "unique_count": int(df[col].nunique()),
                    }
                    if df[col].dtype in ['int64', 'float64']:
                        if not df[col].isna().all():
                            stats["min"] = float(df[col].min())
                            stats["max"] = float(df[col].max())
                            stats["mean"] = float(df[col].mean())
                    else:
                        samples = df[col].dropna().head(5).tolist()
                        stats["samples"] = [str(s)[:50] for s in samples]
                    column_stats[col] = stats
                api_dict["column_stats"] = column_stats
                return api_dict
            except ImportError:
                pass  # cloud package not available — fall through to legacy

        # Legacy path (identical to pre-Phase-3 behavior)
        df, total_rows = await _executor.execute(
            [PipelineStep(**s.model_dump()) for s in request.pipeline],
            limit=request.limit,
            offset=0
        )

        # Get column stats
        column_stats = {}
        for col in df.columns:
            stats = {
                "type": str(df[col].dtype),
                "null_count": int(df[col].isna().sum()),
                "unique_count": int(df[col].nunique()),
            }

            if df[col].dtype in ['int64', 'float64']:
                if not df[col].isna().all():
                    stats["min"] = float(df[col].min())
                    stats["max"] = float(df[col].max())
                    stats["mean"] = float(df[col].mean())
            else:
                # Sample values
                samples = df[col].dropna().head(5).tolist()
                stats["samples"] = [str(s)[:50] for s in samples]

            column_stats[col] = stats

        # Convert to records
        records = df.replace({np.nan: None}).to_dict(orient="records")

        # Convert datetime objects to strings
        for record in records:
            for key, value in record.items():
                if isinstance(value, (datetime, pd.Timestamp)):
                    record[key] = value.isoformat()
                elif hasattr(value, 'item'):  # numpy types
                    record[key] = value.item()

        return {
            "data": records,
            "columns": list(df.columns),
            "column_stats": column_stats,
            "row_count": len(records),
            "total_rows": total_rows,
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/views")
async def list_views() -> List[Dict[str, Any]]:
    """List all derived views."""
    return [
        {"id": vid, **view}
        for vid, view in _derived_views.items()
    ]


@router.get("/views/{view_id}")
async def get_view(view_id: str) -> Dict[str, Any]:
    """Get a specific view."""
    if view_id not in _derived_views:
        raise HTTPException(status_code=404, detail="View not found")
    return {"id": view_id, **_derived_views[view_id]}


@router.post("/views")
async def create_view(view: DerivedViewCreate) -> Dict[str, Any]:
    """Create a new derived view."""
    view_id = str(uuid.uuid4())

    # Extract source tables from pipeline
    source_tables = []
    for step in view.pipeline:
        if step.type == "source":
            source_tables.append(step.config.get("table", ""))
        elif step.type == "join":
            source_tables.append(step.config.get("table", ""))

    view_data = {
        "name": view.name,
        "description": view.description,
        "pipeline": [s.model_dump() for s in view.pipeline],
        "source_tables": list(set(source_tables)),
        "is_materialized": view.is_materialized,
        "tags": view.tags,
        "row_count": None,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
        "created_by": "user",
    }

    # If materialized, execute and store
    if view.is_materialized:
        try:
            df, total_rows = await _executor.execute(
                view.pipeline,
                limit=1000000,
                offset=0
            )
            _materialized_data[view.name] = df
            view_data["row_count"] = total_rows
            view_data["materialized_at"] = datetime.utcnow().isoformat()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to materialize view: {e}")

    _derived_views[view_id] = view_data
    return {"id": view_id, **view_data}


@router.put("/views/{view_id}")
async def update_view(view_id: str, updates: DerivedViewUpdate) -> Dict[str, Any]:
    """Update a derived view."""
    if view_id not in _derived_views:
        raise HTTPException(status_code=404, detail="View not found")

    view = _derived_views[view_id]

    if updates.name is not None:
        view["name"] = updates.name
    if updates.description is not None:
        view["description"] = updates.description
    if updates.pipeline is not None:
        view["pipeline"] = [s.model_dump() for s in updates.pipeline]
        # Re-extract source tables
        source_tables = []
        for step in updates.pipeline:
            if step.type == "source":
                source_tables.append(step.config.get("table", ""))
            elif step.type == "join":
                source_tables.append(step.config.get("table", ""))
        view["source_tables"] = list(set(source_tables))
    if updates.tags is not None:
        view["tags"] = updates.tags

    view["updated_at"] = datetime.utcnow().isoformat()

    return {"id": view_id, **view}


@router.delete("/views/{view_id}")
async def delete_view(view_id: str) -> Dict[str, str]:
    """Delete a derived view."""
    if view_id not in _derived_views:
        raise HTTPException(status_code=404, detail="View not found")

    view = _derived_views[view_id]

    # Remove materialized data if exists
    if view["name"] in _materialized_data:
        del _materialized_data[view["name"]]

    del _derived_views[view_id]

    return {"status": "deleted", "view_id": view_id}


@router.post("/views/{view_id}/materialize")
async def materialize_view(view_id: str) -> Dict[str, Any]:
    """Materialize a virtual view (execute and persist results)."""
    if view_id not in _derived_views:
        raise HTTPException(status_code=404, detail="View not found")

    view = _derived_views[view_id]
    pipeline = [PipelineStep(**s) for s in view["pipeline"]]

    try:
        df, total_rows = await _executor.execute(pipeline, limit=1000000, offset=0)
        _materialized_data[view["name"]] = df

        view["is_materialized"] = True
        view["row_count"] = total_rows
        view["materialized_at"] = datetime.utcnow().isoformat()
        view["updated_at"] = datetime.utcnow().isoformat()

        return {"id": view_id, **view}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to materialize view: {e}")


@router.post("/views/{view_id}/refresh")
async def refresh_view(view_id: str) -> Dict[str, Any]:
    """Refresh a materialized view."""
    return await materialize_view(view_id)


@router.post("/export")
async def export_data(request: ExportRequest) -> Dict[str, Any]:
    """Export query results to a file format."""
    try:
        limit = request.limit or 100000
        df, total_rows = await _executor.execute(
            [PipelineStep(**s.model_dump()) for s in request.pipeline],
            limit=limit,
            offset=0
        )

        if request.format == "csv":
            content = df.to_csv(index=False)
            content_type = "text/csv"
        elif request.format == "json":
            content = df.to_json(orient="records", date_format="iso")
            content_type = "application/json"
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported format:  {request.format}")

        return {
            "format": request.format,
            "row_count": len(df),
            "content": content,
            "content_type": content_type,
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Add this new endpoint to workbench_api.py

class SQLQueryRequest(BaseModel):
    sql: str
    limit: int = 1000


@router.post("/sql")
async def execute_sql(request: SQLQueryRequest) -> Dict[str, Any]:
    """
    Execute a raw SQL-like query.
    Note: This is a simplified SQL parser - not full SQL support.
    For security, only SELECT queries on known tables are allowed.
    """
    sql = request.sql.strip()

    # Basic security check - only allow SELECT
    if not sql.upper().startswith('SELECT'):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")

    # Parse the SQL to extract table name and build a pipeline
    # This is a simplified parser - a real implementation would use a SQL parser library
    try:
        pipeline = parse_sql_to_pipeline(sql, request.limit)

        df, total_rows = await _executor.execute(
            [PipelineStep(**s) for s in pipeline],
            limit=request.limit,
            offset=0
        )

        # Get column stats
        column_stats = {}
        for col in df.columns:
            stats = {
                "type": str(df[col].dtype),
                "null_count": int(df[col].isna().sum()),
                "unique_count": int(df[col].nunique()),
            }
            if df[col].dtype in ['int64', 'float64']:
                if not df[col].isna().all():
                    stats["min"] = float(df[col].min())
                    stats["max"] = float(df[col].max())
                    stats["mean"] = float(df[col].mean())
            column_stats[col] = stats

        # Convert to records
        records = df.replace({np.nan: None}).to_dict(orient="records")

        # Convert datetime objects
        for record in records:
            for key, value in record.items():
                if isinstance(value, (datetime, pd.Timestamp)):
                    record[key] = value.isoformat()
                elif hasattr(value, 'item'):
                    record[key] = value.item()

        return {
            "data": records,
            "columns": list(df.columns),
            "column_stats": column_stats,
            "row_count": len(records),
            "total_rows": total_rows,
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SQL execution failed: {str(e)}")


def parse_sql_to_pipeline(sql: str, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Parse a simple SQL SELECT statement into a pipeline.
    This is a basic parser - supports common patterns but not full SQL.
    """
    import re

    sql_upper = sql.upper()
    sql_clean = ' '.join(sql.split())  # Normalize whitespace

    pipeline = []

    # Extract FROM table
    from_match = re.search(r'\bFROM\s+(\w+)', sql, re.IGNORECASE)
    if not from_match:
        raise ValueError("Could not find FROM clause")

    table_name = from_match.group(1)
    pipeline.append({"type": "source", "config": {"table": table_name}})

    # Extract JOIN if present
    join_pattern = r'(INNER|LEFT|RIGHT|OUTER|FULL)?\s*JOIN\s+(\w+)\s+ON\s+([^WHERE|GROUP|ORDER|LIMIT]+)'
    join_matches = re.finditer(join_pattern, sql, re.IGNORECASE)
    for match in join_matches:
        join_type = (match.group(1) or 'inner').lower()
        join_table = match.group(2)
        on_clause = match.group(3).strip()

        # Parse ON conditions (simple:  a.col = b.col)
        on_conditions = []
        for cond in on_clause.split(' AND '):
            cond = cond.strip()
            if '=' in cond:
                parts = cond.split('=')
                left = parts[0].strip().split('.')[-1]  # Remove table prefix
                right = parts[1].strip().split('.')[-1]
                on_conditions.append({"left": left, "right": right})

        if on_conditions:
            pipeline.append({
                "type": "join",
                "config": {
                    "table": join_table,
                    "type": join_type,
                    "on": on_conditions
                }
            })

    # Extract WHERE conditions
    where_match = re.search(r'\bWHERE\s+(.+?)(?=GROUP BY|ORDER BY|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
    if where_match:
        where_clause = where_match.group(1).strip()
        conditions = []

        # Simple parsing of AND-separated conditions
        for cond in re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE):
            cond = cond.strip()
            if not cond:
                continue

            # Parse condition: field op value
            if ' IS NULL' in cond.upper():
                field = cond.upper().replace(' IS NULL', '').strip()
                conditions.append({"field": field.lower(), "op": "isnull", "value": None})
            elif ' IS NOT NULL' in cond.upper():
                field = cond.upper().replace(' IS NOT NULL', '').strip()
                conditions.append({"field": field.lower(), "op": "notnull", "value": None})
            elif ' LIKE ' in cond.upper():
                parts = re.split(r'\s+LIKE\s+', cond, flags=re.IGNORECASE)
                if len(parts) == 2:
                    field = parts[0].strip()
                    value = parts[1].strip().strip("'\"")
                    if value.startswith('%') and value.endswith('%'):
                        conditions.append({"field": field, "op": "contains", "value": value[1:-1]})
                    elif value.endswith('%'):
                        conditions.append({"field": field, "op": "startswith", "value": value[:-1]})
                    elif value.startswith('%'):
                        conditions.append({"field": field, "op": "endswith", "value": value[1:]})
            elif ' IN ' in cond.upper():
                parts = re.split(r'\s+IN\s+', cond, flags=re.IGNORECASE)
                if len(parts) == 2:
                    field = parts[0].strip()
                    values_str = parts[1].strip()
                    # Parse (val1, val2, ...)
                    values = re.findall(r"'([^']*)'", values_str)
                    conditions.append({"field": field, "op": "in", "value": values})
            else:
                # Standard comparison operators
                for op_sql, op_code in [('>=', 'gte'), ('<=', 'lte'), ('!=', 'ne'), ('=', 'eq'), ('>', 'gt'),
                                        ('<', 'lt')]:
                    if op_sql in cond:
                        parts = cond.split(op_sql)
                        if len(parts) == 2:
                            field = parts[0].strip()
                            value = parts[1].strip().strip("'\"")
                            conditions.append({"field": field, "op": op_code, "value": value})
                            break

        if conditions:
            pipeline.append({"type": "filter", "config": {"conditions": conditions}})

    # Extract GROUP BY
    group_match = re.search(r'\bGROUP BY\s+([^ORDER|LIMIT]+)', sql, re.IGNORECASE)
    if group_match:
        group_fields = [f.strip() for f in group_match.group(1).split(',')]

        # Extract aggregations from SELECT clause
        select_match = re.search(r'\bSELECT\s+(DISTINCT\s+)?(.+?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        aggregations = []

        if select_match:
            select_clause = select_match.group(2)
            # Find aggregation functions
            agg_pattern = r'(COUNT|SUM|AVG|MIN|MAX|FIRST|LAST)\s*\(\s*(\*|\w+)\s*\)\s*(AS\s+(\w+))?'
            for match in re.finditer(agg_pattern, select_clause, re.IGNORECASE):
                func = match.group(1).lower()
                field = match.group(2)
                alias = match.group(4) or f"{func}_{field}"
                aggregations.append({"column": alias, "func": func, "field": field})

        pipeline.append({
            "type": "aggregate",
            "config": {
                "group_by": group_fields,
                "aggregations": aggregations
            }
        })

    # Extract ORDER BY
    order_match = re.search(r'\bORDER BY\s+([^LIMIT]+)', sql, re.IGNORECASE)
    if order_match:
        order_clause = order_match.group(1).strip()
        sort_by = []
        for part in order_clause.split(','):
            part = part.strip()
            if ' DESC' in part.upper():
                field = part.upper().replace(' DESC', '').strip()
                sort_by.append({"field": field.lower(), "direction": "desc"})
            else:
                field = part.upper().replace(' ASC', '').strip()
                sort_by.append({"field": field.lower(), "direction": "asc"})

        if sort_by:
            pipeline.append({"type": "sort", "config": {"by": sort_by}})

    # Extract LIMIT
    limit_match = re.search(r'\bLIMIT\s+(\d+)', sql, re.IGNORECASE)
    if limit_match:
        limit_val = int(limit_match.group(1))
        pipeline.append({"type": "limit", "config": {"n": min(limit_val, limit)}})
    else:
        pipeline.append({"type": "limit", "config": {"n": limit}})

    return pipeline


@router.get("/functions")
async def list_functions() -> Dict[str, Any]:
    """List available transformation functions."""
    return {
        "string_functions": [
            {"name": "upper", "syntax": "UPPER(column)", "description": "Convert to uppercase"},
            {"name": "lower", "syntax": "LOWER(column)", "description": "Convert to lowercase"},
            {"name": "trim", "syntax": "TRIM(column)", "description": "Remove leading/trailing whitespace"},
            {"name": "length", "syntax": "LENGTH(column)", "description": "String length"},
            {"name": "concat", "syntax": "CONCAT(col1, col2, ...)", "description": "Concatenate strings"},
        ],
        "date_functions": [
            {"name": "hour", "syntax": "HOUR(column)", "description": "Extract hour from timestamp"},
            {"name": "date", "syntax": "DATE(column)", "description": "Extract date from timestamp"},
            {"name": "dayofweek", "syntax": "DAYOFWEEK(column)", "description": "Day of week (0=Monday)"},
            {"name": "year", "syntax": "YEAR(column)", "description": "Extract year"},
            {"name": "month", "syntax": "MONTH(column)", "description": "Extract month"},
            {"name": "day", "syntax": "DAY(column)", "description": "Extract day"},
        ],
        "numeric_functions": [
            {"name": "round", "syntax": "ROUND(column, decimals)", "description": "Round to decimal places"},
            {"name": "abs", "syntax": "ABS(column)", "description": "Absolute value"},
            {"name": "floor", "syntax": "FLOOR(column)", "description": "Round down"},
            {"name": "ceil", "syntax": "CEIL(column)", "description": "Round up"},
        ],
        "null_functions": [
            {"name": "isnull", "syntax": "ISNULL(column)", "description": "Check if null"},
            {"name": "notnull", "syntax": "NOTNULL(column)", "description": "Check if not null"},
            {"name": "ifnull", "syntax": "IFNULL(column, default)", "description": "Replace null with default"},
            {"name": "coalesce", "syntax": "COALESCE(col1, col2, ...)", "description": "First non-null value"},
        ],
        "aggregate_functions": [
            {"name": "count", "syntax": "COUNT(column)", "description": "Count non-null values"},
            {"name": "count_distinct", "syntax": "COUNT_DISTINCT(column)", "description": "Count unique values"},
            {"name": "sum", "syntax": "SUM(column)", "description": "Sum of values"},
            {"name": "avg", "syntax": "AVG(column)", "description": "Average of values"},
            {"name": "min", "syntax": "MIN(column)", "description": "Minimum value"},
            {"name": "max", "syntax": "MAX(column)", "description": "Maximum value"},
            {"name": "first", "syntax": "FIRST(column)", "description": "First value in group"},
            {"name": "last", "syntax": "LAST(column)", "description": "Last value in group"},
        ],
        "operators": [
            {"name": "+", "description": "Addition"},
            {"name": "-", "description": "Subtraction"},
            {"name": "*", "description": "Multiplication"},
            {"name": "/", "description": "Division"},
            {"name": "%", "description": "Modulo"},
        ],
        "filter_operators": [
            {"name": "eq", "description": "Equals"},
            {"name": "ne", "description": "Not equals"},
            {"name": "gt", "description": "Greater than"},
            {"name": "gte", "description": "Greater than or equal"},
            {"name": "lt", "description": "Less than"},
            {"name": "lte", "description": "Less than or equal"},
            {"name": "in", "description": "In list"},
            {"name": "nin", "description": "Not in list"},
            {"name": "contains", "description": "Contains substring"},
            {"name": "startswith", "description": "Starts with"},
            {"name": "endswith", "description": "Ends with"},
            {"name": "isnull", "description": "Is null"},
            {"name": "notnull", "description": "Is not null"},
            {"name": "regex", "description": "Matches regex"},
            {"name": "between", "description": "Between two values"},
        ],
    }


@router.delete("/views")
async def delete_all_views() -> Dict[str, Any]:
    """Delete all derived views and materialized data."""
    global _derived_views, _materialized_data

    count = len(_derived_views)
    _derived_views.clear()
    _materialized_data.clear()

    return {
        "deleted": count,
        "message": f"Deleted {count} views and cleared all materialized data"
    }


@router.post("/reset")
async def reset_workbench() -> Dict[str, Any]:
    """Reset workbench state - clear all in-memory data."""
    global _derived_views, _materialized_data

    views_count = len(_derived_views)
    mat_count = len(_materialized_data)

    _derived_views.clear()
    _materialized_data.clear()

    return {
        "views_cleared": views_count,
        "materialized_cleared": mat_count,
        "message": "Workbench state reset successfully"
    }


@router.get("/status")
async def get_workbench_status(
        session: AsyncSession = Depends(get_workbench_session),
) -> Dict[str, Any]:
    """Get workbench status including database connectivity."""
    from sqlalchemy import text, func

    # Check if events table exists and has data
    try:
        result = await session.execute(select(func.count(Event.id)))
        event_count = result.scalar() or 0
    except Exception:
        event_count = 0

    return {
        "views_count": len(_derived_views),
        "materialized_count": len(_materialized_data),
        "database_events": event_count,
        "status": "ok"
    }

@router.get("/engine")
async def get_engine_info() -> Dict[str, Any]:
    """
    Get info about the current execution engine used by the workbench.
    """
    if _USE_ENGINE:
        try:
            from cloud.execution.engine_factory import get_engine

            eng = get_engine()
            health = await eng.health_check()
            return {
                "engine": eng.engine_name(),
                "health": health,
                "feature_flag": True,
            }
        except ImportError:
            pass

    return {
        "engine": "local",
        "health": {"engine": "local", "status": "ok"},
        "feature_flag": False,
        "note": "Cloud execution engine not available — using legacy local executor.",
    }