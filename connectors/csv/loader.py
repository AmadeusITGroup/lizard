# connectors/csv/loader.py
# CSV loader for Lizard that:
# - reads raw travel rows
# - coerces types safely (string/float/int/ISO date)
# - parses 'legs' JSON if present
# - packs travel-specific fields into 'meta'
# - returns a list[dict] ready for EventIn(**payload)

from __future__ import annotations

import csv, io, json, math
from typing import Any, Dict, Iterator, List, Optional

def _str_or_empty(x: Any) -> str:
    if x is None: return ""
    if isinstance(x, float) and math.isnan(x): return ""
    return str(x)

def _float_or_none(x: Any) -> Optional[float]:
    if x is None: return None
    if isinstance(x, float):
        if math.isnan(x): return None
        return x
    try:
        return float(str(x))
    except Exception:
        return None

def _int_or_none(x: Any) -> Optional[int]:
    if x is None: return None
    try:
        return int(float(str(x)))
    except Exception:
        return None

def _parse_legs(s: Any) -> List[Dict[str, Any]]:
    if not s: return []
    if isinstance(s, list): return s
    txt = str(s).strip()
    if not txt or txt == "": return []
    if txt[0] in ("{","["):
        try:
            obj = json.loads(txt)
            return obj if isinstance(obj, list) else []
        except Exception:
            return []
    return []

def _pick_timestamp(row: Dict[str, Any]) -> str:
    """
    Try common timestamp keys.Keep original ISO 'Z' if provided.
    """
    for k in ("ts","timestamp","time","date","datetime","event_ts"):
        v = row.get(k)
        if v: return str(v)
    # last resort: empty (let validation fail upstream if mandatory)
    return ""

def _pick_event_type(row: Dict[str, Any]) -> str:
    for k in ("event_type","etype","type"):
        v = row.get(k)
        if v: return str(v)
    return "event"

def events_from_csv_bytes(content: bytes, encoding: str = "utf-8") -> List[Dict[str, Any]]:
    return list(_iter_events_from_csv(io.StringIO(content.decode(encoding))))

def events_from_csv_text(content: str) -> List[Dict[str, Any]]:
    return list(_iter_events_from_csv(io.StringIO(content)))

def _iter_events_from_csv(f: io.TextIOBase) -> Iterator[Dict[str, Any]]:
    reader = csv.DictReader(f)
    for row in reader:
        yield _build_event_payload(row)

def _build_event_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    # Base fields commonly expected by EventIn
    ts   = _pick_timestamp(row)
    etyp = _pick_event_type(row)

    user_id    = _str_or_empty(row.get("user_id") or row.get("user"))
    account_id = _str_or_empty(row.get("account_id") or row.get("account"))
    device_id  = _str_or_empty(row.get("device_id") or row.get("device"))
    ip         = _str_or_empty(row.get("ip") or row.get("ip_addr"))

    lat = _float_or_none(row.get("geo_lat") or row.get("lat"))
    lon = _float_or_none(row.get("geo_lon") or row.get("lon"))

    country = _str_or_empty(row.get("country"))
    city    = _str_or_empty(row.get("city"))

    amount  = _float_or_none(row.get("amount")) or 0.0

    # Sanitize card_hash to be always a string (avoid NaN -> ValidationError)
    card_hash = _str_or_empty(row.get("card_hash"))

    # Travel fields we keep in meta (raw)
    meta: Dict[str, Any] = {
        "pnr":           _str_or_empty(row.get("pnr")),
        "tkt_number":    _str_or_empty(row.get("tkt_number")),
        "carrier":       _str_or_empty(row.get("carrier")),
        "origin":        _str_or_empty(row.get("origin")),
        "dest":          _str_or_empty(row.get("dest")),
        "dep_time":      _str_or_empty(row.get("dep_time")),
        "arr_time":      _str_or_empty(row.get("arr_time")),
        "pos_country":   _str_or_empty(row.get("pos_country")),
        "issue_country": _str_or_empty(row.get("issue_country")),
        "card_country":  _str_or_empty(row.get("card_country")),
        "advance_hours": _int_or_none(row.get("advance_hours")),
        "stay_nights":   _int_or_none(row.get("stay_nights")),
        "office_id":     _str_or_empty(row.get("office_id")),
        "user_sign":     _str_or_empty(row.get("user_sign")),
        "organization":  _str_or_empty(row.get("organization")),
        "card_hash":     card_hash,
        # NEW: multi-leg details (parsed JSON array)
        "legs":          _parse_legs(row.get("legs")),
    }

    # Build payload expected by your FastAPI schema EventIn
    payload: Dict[str, Any] = {
        "timestamp": ts,
        "etype": etyp,

        "user_id":    user_id,
        "account_id": account_id,
        "device_id":  device_id,
        "ip":         ip,

        # Optional geo & place
        "geo_lat":    lat,
        "geo_lon":    lon,
        "country":    country,
        "city":       city,

        # Monetary
        "amount":     amount,

        # Raw travel fields go under meta
        "meta":       meta,
    }

    # Omit None geo fields if absent
    if payload["geo_lat"] is None: del payload["geo_lat"]
    if payload["geo_lon"] is None: del payload["geo_lon"]

    return payload
