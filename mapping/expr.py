# path: mapping/expr.py
"""
Expression evaluation for data mapping transformations.
Supports both row-level and DataFrame-level operations.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Union
import math
import re
from datetime import datetime
import pandas as pd
import numpy as np


def _is_null(value: Any) -> bool:
    """Check if value is null/empty."""
    if value is None: 
        return True
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    # Handle pandas NA
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return False


def _to_float(x: Any) -> Optional[float]:
    """Convert value to float, handling various formats."""
    if x is None:
        return None
    if isinstance(x, float):
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    if isinstance(x, int):
        return float(x)
    try:
        # Handle European decimal format (1.234,56 -> 1234.56)
        s = str(x).strip()
        if not s:
            return None
        # If has comma and period, determine which is decimal separator
        if "," in s and "." in s:
            # Assume last separator is decimal
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            s = s.replace(",", ".")
        # Remove spaces and currency symbols
        s = re.sub(r"[^\d.\-+eE]", "", s)
        return float(s) if s else None
    except (ValueError, TypeError):
        return None


def _to_int(x:  Any) -> Optional[int]:
    """Convert value to integer."""
    f = _to_float(x)
    if f is None: 
        return None
    return int(f)


def _to_bool(x: Any) -> Optional[bool]:
    """Convert value to boolean."""
    if x is None or _is_null(x):
        return None
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(x)
    s = str(x).lower().strip()
    if s in ("true", "1", "yes", "y", "on", "t"):
        return True
    if s in ("false", "0", "no", "n", "off", "f", ""):
        return False
    return None


def _to_datetime(x: Any, fmt: Optional[str] = None) -> Optional[datetime]:
    """Convert value to datetime."""
    if x is None or _is_null(x):
        return None
    try:
        if isinstance(x, datetime):
            return x
        if isinstance(x, pd.Timestamp):
            return x.to_pydatetime()
        s = str(x).strip()
        if fmt:
            return datetime.strptime(s, fmt)
        # Try common formats
        for f in [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y",
        ]: 
            try:
                return datetime.strptime(s, f)
            except ValueError:
                continue
        # Fallback to pandas
        return pd.to_datetime(s).to_pydatetime()
    except Exception:
        return None


# ============================================================
# Unary operations (applied to single values)
# ============================================================

def _apply_unary(val: Any, expr_name: str) -> Any:
    """Apply a unary operation to a value."""
    if expr_name == "try_float":
        return _to_float(val)

    if expr_name == "try_int":
        return _to_int(val)

    if expr_name == "lower":
        return str(val).lower() if not _is_null(val) else None

    if expr_name == "upper":
        return str(val).upper() if not _is_null(val) else None

    if expr_name == "trim":
        return str(val).strip() if not _is_null(val) else None

    if expr_name == "strip":
        return str(val).strip() if not _is_null(val) else None

    if expr_name == "str": 
        if _is_null(val):
            return ""
        return str(val)

    if expr_name == "bool":
        return _to_bool(val)

    if expr_name.startswith("coalesce: "):
        fallback = expr_name.split(":", 1)[1]
        return fallback if _is_null(val) else val

    if expr_name.startswith("default:"):
        fallback = expr_name.split(":", 1)[1]
        return fallback if _is_null(val) else val

    if expr_name.startswith("prefix:"):
        prefix = expr_name.split(":", 1)[1]
        return f"{prefix}{val}" if not _is_null(val) else None

    if expr_name.startswith("suffix:"):
        suffix = expr_name.split(":", 1)[1]
        return f"{val}{suffix}" if not _is_null(val) else None

    return val


# ============================================================
# Template expressions (${colName} substitution)
# ============================================================

_TEMPLATE_RE = re.compile(r"\$\{([A-Za-z0-9_]+)\}")


def _apply_template(expr: str, row: Dict[str, Any]) -> str:
    """Apply template substitution:  ${colName} -> value from row."""

    def repl(m):
        key = m.group(1)
        v = row.get(key, "")
        if _is_null(v):
            return ""
        return str(v)

    return _TEMPLATE_RE.sub(repl, expr)


# ============================================================
# Structured operations (dict-based expressions)
# ============================================================

def _apply_structured(expr_obj: Dict[str, Any], row: Dict[str, Any]) -> Any:
    """Apply a structured expression operation."""
    op = expr_obj.get("op")

    # Concatenation
    if op == "concat": 
        sep = expr_obj.get("sep", "")
        cols:  List[str] = expr_obj.get("cols", [])
        parts:  List[str] = []
        for c in cols:
            v = row.get(c, "")
            if _is_null(v):
                v = ""
            parts.append(str(v))
        return sep.join(parts)

    # Coalesce - first non-null value
    if op == "coalesce": 
        cols: List[str] = expr_obj.get("cols", [])
        for c in cols: 
            v = row.get(c)
            if not _is_null(v):
                return v
        return expr_obj.get("default")

    # Type conversions
    if op == "try_float":
        col = expr_obj.get("col")
        return _to_float(row.get(col))

    if op == "try_int":
        col = expr_obj.get("col")
        return _to_int(row.get(col))

    if op == "bool":
        col = expr_obj.get("col")
        return _to_bool(row.get(col))

    if op == "parse_date":
        col = expr_obj.get("col")
        fmt = expr_obj.get("format")
        return _to_datetime(row.get(col), fmt)

    # String operations
    if op == "lower":
        col = expr_obj.get("col")
        v = row.get(col)
        return str(v).lower() if not _is_null(v) else None

    if op == "upper":
        col = expr_obj.get("col")
        v = row.get(col)
        return str(v).upper() if not _is_null(v) else None

    if op == "trim":
        col = expr_obj.get("col")
        v = row.get(col)
        return str(v).strip() if not _is_null(v) else None

    if op == "substring":
        col = expr_obj.get("col")
        start = expr_obj.get("start", 0)
        end = expr_obj.get("end")
        length = expr_obj.get("length")
        v = row.get(col)
        if _is_null(v):
            return None
        s = str(v)
        if length is not None:
            return s[start:start + length]
        return s[start: end] if end else s[start:]

    if op == "replace":
        col = expr_obj.get("col")
        pattern = expr_obj.get("pattern", "")
        replacement = expr_obj.get("replacement", "")
        v = row.get(col)
        if _is_null(v):
            return None
        return str(v).replace(pattern, replacement)

    if op == "regex_replace":
        col = expr_obj.get("col")
        pattern = expr_obj.get("pattern", "")
        replacement = expr_obj.get("replacement", "")
        v = row.get(col)
        if _is_null(v):
            return None
        try:
            return re.sub(pattern, replacement, str(v))
        except re.error:
            return str(v)

    if op == "regex_extract":
        col = expr_obj.get("col")
        pattern = expr_obj.get("pattern", "")
        group = expr_obj.get("group", 0)
        v = row.get(col)
        if _is_null(v):
            return None
        try: 
            match = re.search(pattern, str(v))
            if match:
                return match.group(group)
        except (re.error, IndexError):
            pass
        return None

    if op == "split":
        col = expr_obj.get("col")
        sep = expr_obj.get("sep", ",")
        index = expr_obj.get("index", 0)
        v = row.get(col)
        if _is_null(v):
            return None
        parts = str(v).split(sep)
        if 0 <= index < len(parts):
            return parts[index].strip()
        return None

    # Conditional
    if op == "if_else" or op == "if":
        col = expr_obj.get("col")
        condition = expr_obj.get("condition", "eq")
        compare_value = expr_obj.get("value")
        then_value = expr_obj.get("then")
        else_value = expr_obj.get("else")
        v = row.get(col)

        result = False
        if condition == "eq": 
            result = v == compare_value
        elif condition == "ne":
            result = v != compare_value
        elif condition == "contains":
            result = str(compare_value) in str(v) if not _is_null(v) else False
        elif condition == "startswith":
            result = str(v).startswith(str(compare_value)) if not _is_null(v) else False
        elif condition == "endswith":
            result = str(v).endswith(str(compare_value)) if not _is_null(v) else False
        elif condition == "is_null":
            result = _is_null(v)
        elif condition == "is_not_null":
            result = not _is_null(v)
        elif condition == "gt":
            try:
                result = float(v) > float(compare_value)
            except (ValueError, TypeError):
                result = False
        elif condition == "gte":
            try:
                result = float(v) >= float(compare_value)
            except (ValueError, TypeError):
                result = False
        elif condition == "lt":
            try:
                result = float(v) < float(compare_value)
            except (ValueError, TypeError):
                result = False
        elif condition == "lte":
            try: 
                result = float(v) <= float(compare_value)
            except (ValueError, TypeError):
                result = False
        elif condition == "in":
            result = v in (compare_value if isinstance(compare_value, list) else [compare_value])
        elif condition == "regex":
            try:
                result = bool(re.search(str(compare_value), str(v)))
            except re.error:
                result = False

        return then_value if result else else_value

    # Math operations
    if op == "add":
        cols = expr_obj.get("cols", [])
        constant = expr_obj.get("constant", 0)
        total = constant
        for c in cols:
            v = _to_float(row.get(c))
            if v is not None:
                total += v
        return total

    if op == "multiply":
        cols = expr_obj.get("cols", [])
        constant = expr_obj.get("constant", 1)
        result = constant
        for c in cols:
            v = _to_float(row.get(c))
            if v is not None: 
                result *= v
        return result

    if op == "divide":
        numerator = expr_obj.get("numerator")
        denominator = expr_obj.get("denominator")
        n = _to_float(row.get(numerator))
        d = _to_float(row.get(denominator))
        if n is None or d is None or d == 0:
            return None
        return n / d

    if op == "round":
        col = expr_obj.get("col")
        decimals = expr_obj.get("decimals", 2)
        v = _to_float(row.get(col))
        if v is None:
            return None
        return round(v, decimals)

    if op == "abs":
        col = expr_obj.get("col")
        v = _to_float(row.get(col))
        if v is None:
            return None
        return abs(v)

    # Mapping/lookup
    if op == "map" or op == "lookup":
        col = expr_obj.get("col")
        mapping = expr_obj.get("mapping", {})
        default = expr_obj.get("default")
        v = row.get(col)
        if _is_null(v):
            return default
        return mapping.get(str(v), default)

    return None


# ============================================================
# Main API
# ============================================================

def apply_mapping_with_expr(
        src: Dict[str, Any],
        mapping: Dict[str, Any],
        target_key: str,
        default: Any = None,
) -> Any:
    """
    Apply mapping to extract a target field value from source row.

    - Get raw value from mapped column if present
    - Apply expression if mapping['__expr__'][target_key] exists: 
        * string template: "${colA}-${colB}"
        * structured:  {"op":"concat","sep": "-","cols": ["colA","colB"]}
        * unary: "try_float" / "lower" / "upper" / "trim" / "bool" / "coalesce: <v>"
    """
    col = mapping.get(target_key)
    val = src.get(col) if col else None

    exprs = mapping.get("__expr__") or {}
    expr = exprs.get(target_key)

    if isinstance(expr, str):
        if "${" in expr:
            val = _apply_template(expr, src)
        else:
            val = _apply_unary(val, expr)
    elif isinstance(expr, dict):
        result = _apply_structured(expr, src)
        if result is not None:
            val = result

    # Return default if val is empty/null
    if _is_null(val):
        return default

    return val


def evaluate_row(
        row: Dict[str, Any],
        mapping: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Evaluate all mappings for a row and return mapped values.
    """
    result = {}
    exprs = mapping.get("__expr__", {})

    for target, source in mapping.items():
        if target == "__expr__":
            continue

        # Get base value
        val = row.get(source) if source else None

        # Apply expression if exists
        if target in exprs:
            expr = exprs[target]
            if isinstance(expr, str):
                if "${" in expr:
                    val = _apply_template(expr, row)
                else: 
                    val = _apply_unary(val, expr)
            elif isinstance(expr, dict):
                structured_val = _apply_structured(expr, row)
                if structured_val is not None:
                    val = structured_val

        result[target] = val

    return result


def evaluate_dataframe(
        df: pd.DataFrame,
        mapping: Dict[str, Any],
) -> pd.DataFrame:
    """
    Apply mapping to entire DataFrame.
    Returns new DataFrame with mapped columns.
    """
    if df.empty:
        return pd.DataFrame(columns=list(mapping.keys()))

    result_rows = []
    for _, row in df.iterrows():
        result_rows.append(evaluate_row(row.to_dict(), mapping))

    return pd.DataFrame(result_rows)


# ============================================================
# Expression builder helpers
# ============================================================

def build_concat_expr(cols: List[str], sep: str = "") -> Dict[str, Any]:
    """Build a concat expression."""
    return {"op": "concat", "cols": cols, "sep": sep}


def build_coalesce_expr(cols: List[str], default: Any = None) -> Dict[str, Any]:
    """Build a coalesce expression."""
    return {"op": "coalesce", "cols": cols, "default": default}


def build_if_expr(
        col: str,
        condition: str,
        value: Any,
        then_val: Any,
        else_val: Any = None
) -> Dict[str, Any]:
    """Build a conditional expression."""
    return {
        "op": "if_else",
        "col": col,
        "condition": condition,
        "value": value,
        "then": then_val,
        "else": else_val,
    }


def build_map_expr(col: str, mapping: Dict[str, Any], default: Any = None) -> Dict[str, Any]:
    """Build a lookup/map expression."""
    return {"op": "map", "col":  col, "mapping": mapping, "default": default}


# ============================================================
# Available expressions documentation
# ============================================================

EXPRESSION_DOCS = {
    "unary": {
        "try_float": "Convert to float (handles comma decimals)",
        "try_int": "Convert to integer",
        "lower": "Convert to lowercase",
        "upper": "Convert to uppercase",
        "trim": "Remove leading/trailing whitespace",
        "str": "Convert to string",
        "bool": "Convert to boolean (true/1/yes -> True, false/0/no -> False)",
        "coalesce: <value>": "Use <value> if null",
        "default: <value>": "Use <value> if null",
        "prefix: <text>": "Add prefix to value",
        "suffix:<text>": "Add suffix to value",
    },
    "structured": {
        "concat": {"op": "concat", "cols":  ["col1", "col2"], "sep": "-"},
        "coalesce": {"op": "coalesce", "cols": ["col1", "col2"], "default": "N/A"},
        "try_float": {"op": "try_float", "col": "amount"},
        "try_int":  {"op": "try_int", "col": "count"},
        "bool": {"op": "bool", "col": "is_active"},
        "parse_date": {"op": "parse_date", "col": "date", "format": "%Y-%m-%d"},
        "substring": {"op": "substring", "col": "text", "start": 0, "length": 10},
        "replace": {"op": "replace", "col": "text", "pattern": "old", "replacement": "new"},
        "regex_extract": {"op": "regex_extract", "col": "text", "pattern": r"(\d+)", "group": 1},
        "split": {"op": "split", "col": "text", "sep":  ",", "index": 0},
        "if_else": {"op": "if_else", "col": "status", "condition": "eq", "value": "active", "then": 1, "else": 0},
        "map": {"op":  "map", "col": "code", "mapping": {"A": "Active", "I": "Inactive"}, "default": "Unknown"},
        "add": {"op": "add", "cols": ["price", "tax"], "constant": 0},
        "multiply": {"op": "multiply", "cols": ["qty", "price"]},
        "round": {"op": "round", "col": "amount", "decimals": 2},
    },
    "template": "${firstName} ${lastName}",
}