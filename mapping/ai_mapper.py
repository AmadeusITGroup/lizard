# path: mapping/ai_mapper.py
"""
AI-assisted schema mapping for data ingestion.
Supports heuristic, OpenAI, and Ollama-based mapping.
Enhanced with comprehensive patterns for fraud scenario data.
"""

from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
import os
import re
import json

import pandas as pd

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OLLAMA_BASE = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3. 1")

# -------------------------------
# Target schema fields
# -------------------------------
LIZARD_FIELDS: List[str] = [
    # Core
    "ts", "source_value", "event_type",

    # Identity & context
    "user_id", "account_id", "device_id", "session_id", "request_id", "ip", "user_agent",

    # Travel/ticketing specifics
    "pnr", "tkt_number", "carrier",
    "origin", "dest", "dep_time", "arr_time", "legs",
    "office_id", "user_sign", "organization",

    # Payment
    "amount", "currency", "payment_method", "card_hash",
    "fop_type", "fop_name", "fop_subtype", "card_last4", "card_bin",

    # Geo
    "geo_lat", "geo_lon", "country", "city", "region",
    "pos_country", "issue_country", "card_country",

    # Stay/travel details
    "advance_hours", "stay_nights",

    # Other business/generic
    "order_id", "refund_id", "status", "failure_reason",

    # Fraud detection flags
    "is_unusual", "is_vpn", "is_fraud_indicator",
]

# Field metadata for UI and validation
FIELD_METADATA: Dict[str, Dict[str, Any]] = {
    "ts": {"type": "datetime", "required": True, "description": "Event timestamp (ISO 8601)"},
    "event_type": {"type": "string", "required": True, "description": "Type/category of event"},
    "user_id": {"type": "string", "required": False, "description": "User identifier"},
    "account_id": {"type": "string", "required": False, "description": "Account/customer ID"},
    "device_id": {"type": "string", "required": False, "description": "Device fingerprint/ID"},
    "session_id": {"type": "string", "required": False, "description": "Session identifier"},
    "request_id": {"type": "string", "required": False, "description": "Request/correlation ID"},
    "ip": {"type": "string", "required": False, "description": "IP address (v4 or v6)"},
    "user_agent": {"type": "string", "required": False, "description": "Browser/client user agent"},
    "pnr": {"type": "string", "required": False, "description": "Booking reference (PNR/record locator)"},
    "tkt_number": {"type": "string", "required": False, "description": "Ticket number"},
    "carrier": {"type": "string", "required": False, "description": "Airline/carrier code"},
    "origin": {"type": "string", "required": False, "description": "Origin airport/city code"},
    "dest": {"type": "string", "required": False, "description": "Destination airport/city code"},
    "dep_time": {"type": "datetime", "required": False, "description": "Departure time"},
    "arr_time": {"type": "datetime", "required": False, "description": "Arrival time"},
    "legs": {"type": "json", "required": False, "description": "Flight legs (JSON array)"},
    "office_id": {"type": "string", "required": False, "description": "Office/agent identifier"},
    "user_sign": {"type": "string", "required": False, "description": "User sign/agent code"},
    "organization": {"type": "string", "required": False, "description": "Organization/agency name"},
    "amount": {"type": "number", "required": False, "description": "Transaction amount"},
    "currency": {"type": "string", "required": False, "description": "Currency code (ISO 4217)"},
    "payment_method": {"type": "string", "required": False, "description": "Payment method/FOP"},
    "card_hash": {"type": "string", "required": False, "description": "Hashed payment card"},
    "fop_type": {"type": "string", "required": False, "description": "Form of payment type code"},
    "fop_name": {"type": "string", "required": False, "description": "Form of payment name"},
    "fop_subtype": {"type": "string", "required": False, "description": "Form of payment subtype (VI, MC, etc.)"},
    "card_last4": {"type": "string", "required": False, "description": "Last 4 digits of card"},
    "card_bin": {"type": "string", "required": False, "description": "Card BIN (first 6 digits)"},
    "geo_lat": {"type": "number", "required": False, "description": "Latitude (-90 to 90)"},
    "geo_lon": {"type": "number", "required": False, "description": "Longitude (-180 to 180)"},
    "country": {"type": "string", "required": False, "description": "Country code (ISO 3166)"},
    "city": {"type": "string", "required": False, "description": "City name"},
    "region": {"type": "string", "required": False, "description": "Region/state/province"},
    "pos_country": {"type": "string", "required": False, "description": "Point of sale country"},
    "issue_country": {"type": "string", "required": False, "description": "Ticket issue country"},
    "card_country": {"type": "string", "required": False, "description": "Card issuer country"},
    "advance_hours": {"type": "number", "required": False, "description": "Hours before departure"},
    "stay_nights": {"type": "number", "required": False, "description": "Number of nights stay"},
    "order_id": {"type": "string", "required": False, "description": "Order identifier"},
    "refund_id": {"type": "string", "required": False, "description": "Refund identifier"},
    "status": {"type": "string", "required": False, "description": "Event status/result"},
    "failure_reason": {"type": "string", "required": False, "description": "Reason for failure/rejection"},
    "is_unusual": {"type": "boolean", "required": False, "description": "Pre-flagged as unusual"},
    "is_vpn": {"type": "boolean", "required": False, "description": "Connection via VPN detected"},
    "is_fraud_indicator": {"type": "boolean", "required": False, "description": "Fraud indicator flag"},
    "source_value": {"type": "string", "required": False, "description": "Data source name"},
}

# -------------------------------
# Synonym patterns for fuzzy matching
# Enhanced for fraud scenario data
# -------------------------------
SYN: Dict[str, List[str]] = {
    # Core fields
    "ts": [
        r"\b(ts|timestamp|time|datetime|event[_]?time|@timestamp|created[_]?at|occurred[_]?at|date)\b"
    ],
    "event_type": [
        r"\b(event[_]?type|etype|type|action|event[_]?name|operation|activity)\b"
    ],

    # Identity fields
    "user_id": [
        r"\b(user[_]?id|userid|uid|username|login|user|principal|actor|lss[_]?user|sign[_]?user|customer[_]?id)\b"
    ],
    "account_id": [
        r"\b(account[_]?id|acct[_]?id|account|customer|cust[_]?id|client[_]?id)\b"
    ],
    "device_id": [
        r"\b(device[_]? id|did|device|device[_]?fingerprint|fingerprint|dfp|dev[_]?id|hardware[_]?id)\b"
    ],
    "session_id": [
        r"\b(session[_]?id|sid|session|sess[_]?id|visit[_]?id)\b"
    ],
    "request_id": [
        r"\b(request[_]? id|rid|req[_]?id|trace[_]?id|correlation[_]?id|transaction[_]?id)\b"
    ],
    "ip": [
        r"\b(ip|ip[_]?addr(ess)?|client[_]?ip|source[_]?ip|src[_]?ip|remote[_]?ip|user[_]?ip)\b"
    ],
    "user_agent": [
        r"\b(user[_]?agent|ua|agent|http[_]?user[_]?agent|browser)\b"
    ],

    # Travel/ticketing fields
    "pnr": [
        r"\b(pnr|booking[_]?id|reservation[_]?id|recloc|reclocator|confirmation[_]?number|booking[_]?ref)\b"
    ],
    "tkt_number": [
        r"\b(tkt[_]?number|ticket[_]?number|eticket[_]?number|document[_]?number|ticket[_]?no)\b"
    ],
    "carrier": [
        r"\b(carrier|airline[_]?code|marketing[_]?carrier|operating[_]?carrier|airline)\b"
    ],
    "origin": [
        r"\b(origin|orig|from|departure[_]?airport|dep[_]?airport|departure[_]?city|dep[_]?city)\b"
    ],
    "dest": [
        r"\b(dest|destination|to|arrival[_]? airport|arr[_]?airport|arrival[_]?city|arr[_]?city)\b"
    ],
    "dep_time": [
        r"\b(dep[_]?time|departure[_]?time|depart[_]?time|flight[_]?departure|std|scheduled[_]?departure)\b"
    ],
    "arr_time": [
        r"\b(arr[_]?time|arrival[_]? time|flight[_]?arrival|sta|scheduled[_]?arrival)\b"
    ],
    "legs": [
        r"\b(legs|segments|itinerary|flight[_]?legs|routing|flight[_]?segments)\b"
    ],
    "office_id": [
        r"\b(office[_]? id|office|issuing[_]?office|agent[_]?office|oid|pos[_]?id|agency[_]?id)\b"
    ],
    "user_sign": [
        r"\b(user[_]?sign|agent[_]?sign|sign|agent[_]?code|sine|agent[_]?id)\b"
    ],
    "organization": [
        r"\b(organization|agency|company|org|iata[_]?number|agency[_]?name|org[_]?name)\b"
    ],

    # Payment fields
    "amount": [
        r"\b(amount|value|price|fare|total[_]?amount|amt|transaction[_]?amount|sum|total)\b"
    ],
    "currency": [
        r"\b(currency|ccy|cur|iso[_]?currency|currency[_]?code)\b"
    ],
    "payment_method": [
        r"\b(payment[_]? method|pay[_]?method|fop|form[_]?of[_]?payment|payment[_]? type)\b"
    ],
    "card_hash": [
        r"\b(card[_]? hash|pan[_]?hash|cc[_]?hash|card[_]?fingerprint|pan|card[_]?token)\b"
    ],
    "fop_type": [
        r"\b(fop[_]?type|fop[_]?code|payment[_]?type[_]?code)\b"
    ],
    "fop_name": [
        r"\b(fop[_]?name|payment[_]?type[_]?name)\b"
    ],
    "fop_subtype": [
        r"\b(fop[_]?subtype|card[_]?type|card[_]?brand)\b"
    ],
    "card_last4": [
        r"\b(card[_]?last[_]?4|last[_]?4|card[_]?suffix)\b"
    ],
    "card_bin": [
        r"\b(card[_]? bin|bin|card[_]?prefix|issuer[_]?id)\b"
    ],

    # Geo fields
    "geo_lat": [
        r"\b(geo[_]? lat|lat|latitude|y[_]?coord)\b"
    ],
    "geo_lon": [
        r"\b(geo[_]? lon|lng|lon|longitude|x[_]?coord)\b"
    ],
    "country": [
        r"\b(country|country[_]?code|country[_]?iso2|ctry|nation)\b"
    ],
    "city": [
        r"\b(city|locality|town|municipality)\b"
    ],
    "region": [
        r"\b(region|state|province|county|district)\b"
    ],
    "pos_country": [
        r"\b(pos[_]?country|pos[_]?ctry|pos[_]?country[_]?code|point[_]?of[_]?sale[_]?country)\b"
    ],
    "issue_country": [
        r"\b(issue[_]?country|issue[_]?ctry|issue[_]?country[_]?code|issuing[_]?country)\b"
    ],
    "card_country": [
        r"\b(card[_]?country|card[_]?ctry|card[_]?country[_]?code|bin[_]?country|issuer[_]?country)\b"
    ],

    # Stay/travel details
    "advance_hours": [
        r"\b(advance[_]?hours|advance|lead[_]?time|hours[_]?to[_]?departure|booking[_]?lead|advance[_]?purchase)\b"
    ],
    "stay_nights": [
        r"\b(stay[_]?nights|nights|stay|length[_]?of[_]?stay|los|duration[_]?nights)\b"
    ],

    # Business fields
    "order_id": [
        r"\b(order[_]?id|order|order[_]?number|purchase[_]?id)\b"
    ],
    "refund_id": [
        r"\b(refund[_]?id|refund|refund[_]?number|reversal[_]?id)\b"
    ],
    "status": [
        r"\b(status|result|outcome|state|response[_]?code|event[_]?status)\b"
    ],
    "failure_reason": [
        r"\b(failure[_]? reason|fail[_]?reason|error[_]?reason|reject[_]?reason|decline[_]?reason|error[_]?message)\b"
    ],

    # Fraud detection flags
    "is_unusual": [
        r"\b(is[_]?unusual|unusual|anomaly|anomalies|flag|suspicious)\b"
    ],
    "is_vpn": [
        r"\b(is[_]?vpn|vpn|vpn[_]?detected|proxy|tor|vpn[_]?flag)\b"
    ],
    "is_fraud_indicator": [
        r"\b(is[_]?fraud[_]? indicator|fraud[_]?indicator|fraud[_]?flag|is[_]?fraud|fraud[_]?score|risk[_]?flag)\b"
    ],
}


# -------------------------------
# Helper functions
# -------------------------------
def _normalize_cols(df: pd.DataFrame) -> Dict[str, str]:
    """Create lowercase -> original case mapping of column names."""
    return {str(c).lower().strip(): str(c) for c in df.columns}


def _find(cols: Dict[str, str], patterns: List[str], default: Optional[str] = None) -> Optional[str]:
    """
    Find a column matching any of the patterns.
    Returns original-case column name or default.
    """
    for p in patterns:
        rx = re.compile(p, re.IGNORECASE)
        for k, v in cols.items():
            if rx.search(k):
                return v
    return default


def _score_column_match(col_name: str, patterns: List[str]) -> float:
    """Score how well a column matches patterns (0-1)."""
    col_lower = col_name.lower().strip()
    max_score = 0.0

    for p in patterns:
        rx = re.compile(p, re.IGNORECASE)
        match = rx.search(col_lower)
        if match:
            # Score based on match coverage
            match_len = match.end() - match.start()
            coverage = match_len / len(col_lower) if col_lower else 0
            # Bonus for exact match
            if col_lower == match.group().lower():
                coverage = 1.0
            max_score = max(max_score, coverage)

    return max_score


def analyze_columns(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Analyze DataFrame columns and return metadata about each.
    Useful for UI to show data types and sample values.
    """
    analysis = {}

    for col in df.columns:
        col_data = df[col]
        non_null = col_data.dropna()

        # Detect type
        if pd.api.types.is_numeric_dtype(col_data):
            detected_type = "number"
        elif pd.api.types.is_datetime64_any_dtype(col_data):
            detected_type = "datetime"
        elif pd.api.types.is_bool_dtype(col_data):
            detected_type = "boolean"
        else:
            # Check if it looks like a boolean
            str_vals = col_data.astype(str).str.lower().unique()
            bool_vals = {'true', 'false', '0', '1', 'yes', 'no', 'nan', ''}
            if len(set(str_vals) - bool_vals) == 0:
                detected_type = "boolean"
            else:
                detected_type = "string"

        # Sample values
        samples = non_null.head(5).tolist() if len(non_null) > 0 else []

        # Unique count
        unique_count = col_data.nunique()

        analysis[col] = {
            "detected_type": detected_type,
            "null_count": int(col_data.isna().sum()),
            "null_percent": round(col_data.isna().mean() * 100, 2),
            "unique_count": int(unique_count),
            "sample_values": [str(s)[:100] for s in samples],
        }

    return analysis


def suggest_mapping_with_scores(df: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
    """
    Suggest mappings with confidence scores for each target field.
    Returns dict of target_field -> list of {column, score} candidates.
    """
    cols = _normalize_cols(df)
    suggestions = {}

    for target, patterns in SYN.items():
        candidates = []
        for col_lower, col_original in cols.items():
            score = _score_column_match(col_lower, patterns)
            if score > 0.1:  # Minimum threshold
                candidates.append({
                    "column": col_original,
                    "score": round(score, 3),
                })

        # Sort by score descending
        candidates.sort(key=lambda x: -x["score"])
        suggestions[target] = candidates[: 5]  # Top 5 candidates

    return suggestions


# -------------------------------
# Heuristic mapping
# -------------------------------
def _heuristic_map(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate mapping using heuristic pattern matching.
    Fast, no external dependencies.
    """
    cols = _normalize_cols(df)
    headers = list(df.columns)
    mapping: Dict[str, Any] = {}

    # Required fields with fallbacks
    mapping["ts"] = _find(cols, SYN["ts"], default=headers[0] if headers else None)
    mapping["event_type"] = _find(cols, SYN["event_type"], default="event")

    # Best-effort for all other targets
    for k in LIZARD_FIELDS:
        if k in ("ts", "event_type", "source_value"):
            continue
        hit = _find(cols, SYN.get(k, []))
        if hit:
            mapping[k] = hit

    # Provide default for source_value
    mapping["source_value"] = "csv_upload"

    # Build expressions for common transformations
    expr: Dict[str, Any] = {}

    # Normalize numeric fields
    for num_key in ("amount", "geo_lat", "geo_lon", "advance_hours", "stay_nights"):
        if mapping.get(num_key):
            expr[num_key] = "try_float"

    # Auto-generate user_id from parts if missing
    if not mapping.get("user_id"):
        parts = [p for p in (mapping.get("account_id"), mapping.get("device_id")) if p]
        if len(parts) >= 2:
            expr["user_id"] = {"op": "concat", "sep": "-", "cols": parts}
        elif mapping.get("user_sign"):
            mapping["user_id"] = mapping["user_sign"]

    # Uppercase country/currency codes
    for code_field in ("country", "currency", "carrier", "origin", "dest", "pos_country", "card_country",
                       "issue_country"):
        if mapping.get(code_field):
            expr[code_field] = "upper"

    # Boolean conversions
    for bool_field in ("is_unusual", "is_vpn", "is_fraud_indicator"):
        if mapping.get(bool_field):
            expr[bool_field] = "bool"

    if expr:
        mapping["__expr__"] = expr

    return mapping


# -------------------------------
# LLM-based mapping
# -------------------------------
def _prompt_from_schema(df: pd.DataFrame) -> str:
    """Generate prompt for LLM-based mapping."""
    column_info = []
    for col in df.columns:
        samples = df[col].dropna().head(3).tolist()
        sample_str = ", ".join([str(s)[:50] for s in samples])
        column_info.append(f"  - {col}: [{sample_str}]")

    columns_text = "\n".join(column_info)

    return f"""You are a data mapping expert. Map CSV columns to a target schema for fraud/ticketing analytics.

Target schema fields:
{json.dumps(LIZARD_FIELDS, indent=2)}

Source columns with sample values:
{columns_text}

Return STRICT JSON only, no explanation. Format:
{{
  "ts": "source_column_name",
  "event_type": "source_column_name",
  "user_id": "source_column_name",
  ... 
  "__expr__": {{
    "amount": "try_float",
    "user_id": {{"op": "concat", "sep":  "-", "cols": ["col1", "col2"]}}
  }}
}}

Rules:
- Map each target field to the best matching source column
- Omit fields with no good match (don't invent columns)
- Use __expr__ for transformations:  
  - "try_float" for numeric conversion
  - "upper"/"lower" for case conversion
  - "bool" for boolean conversion
  - {{"op": "concat", "sep":  "-", "cols": [...]}} for concatenation
- Set source_value to a descriptive name for this data source
"""


def _openai_map(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate mapping using OpenAI API."""
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY not set")

    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    prompt = _prompt_from_schema(df)

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
    )
    text = resp.choices[0].message.content

    # Extract JSON from response
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON block in response
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            return json.loads(match.group(0))
        raise ValueError(f"Could not parse JSON from OpenAI response: {text[: 200]}")


def _ollama_map(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate mapping using Ollama (local LLM)."""
    import requests

    prompt = _prompt_from_schema(df)
    r = requests.post(
        f"{OLLAMA_BASE}/api/generate",
        json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
        timeout=120
    )
    r.raise_for_status()
    payload = r.json()

    try:
        return json.loads(payload["response"])
    except json.JSONDecodeError:
        # Try to find JSON block
        match = re.search(r"\{[\s\S]*\}", payload["response"])
        if match:
            return json.loads(match.group(0))
        raise ValueError(f"Could not parse JSON from Ollama response")


# -------------------------------
# Public API
# -------------------------------
def suggest_event_mapping(
        df: pd.DataFrame,
        engine: str = "heuristic"
) -> Dict[str, Any]:
    """
    Suggest mapping for a DataFrame.

    Args:
        df: Source DataFrame to map
        engine: "heuristic", "openai", or "ollama"

    Returns:
        Mapping dict with target_field -> source_column mappings
        and optional __expr__ for transformations
    """
    if engine == "openai" and OPENAI_API_KEY:
        try:
            return _openai_map(df)
        except Exception as e:
            print(f"OpenAI mapping failed, falling back to heuristic: {e}")
            return _heuristic_map(df)

    if engine == "ollama":
        try:
            return _ollama_map(df)
        except Exception as e:
            print(f"Ollama mapping failed, falling back to heuristic: {e}")
            return _heuristic_map(df)

    return _heuristic_map(df)


def get_field_metadata() -> Dict[str, Dict[str, Any]]:
    """Get metadata for all target fields."""
    return FIELD_METADATA


def get_target_fields() -> List[str]:
    """Get list of all target field names."""
    return LIZARD_FIELDS