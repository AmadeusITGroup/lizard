# path: domain/schemas.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Literal
from datetime import datetime
from pydantic import BaseModel, Field


class EventIn(BaseModel):
    ts: datetime
    source: str
    event_type: str
    user_id: Optional[str] = None
    account_id: Optional[str] = None
    device_id: Optional[str] = None
    card_hash: Optional[str] = None
    ip: Optional[str] = None
    geo_lat: Optional[float] = None
    geo_lon: Optional[float] = None
    country: Optional[str] = None
    city: Optional[str] = None
    is_unusual: Optional[bool] = False

    # NEW:  Ticket fields
    office_id: Optional[str] = None
    user_sign: Optional[str] = None
    organization: Optional[str] = None
    pnr: Optional[str] = None
    carrier: Optional[str] = None
    origin: Optional[str] = None
    dest: Optional[str] = None
    tkt_number: Optional[str] = None
    status: Optional[str] = None
    pos_country: Optional[str] = None
    card_country: Optional[str] = None
    advance_hours: Optional[float] = None
    stay_nights: Optional[int] = None
    amount: Optional[float] = None
    currency: Optional[str] = None
    fop_type: Optional[str] = None
    fop_name: Optional[str] = None
    fop_subtype: Optional[str] = None
    card_last4: Optional[str] = None
    card_bin: Optional[str] = None
    is_fraud_indicator: Optional[bool] = None
    failure_reason: Optional[str] = None
    legs: Optional[str] = None  # JSON string

    meta: Dict[str, Any] = Field(default_factory=dict)


class EventOut(EventIn):
    pass

class EventOut(EventIn):
    pass


class EntityIn(BaseModel):
    type: str
    key: str
    props: Dict[str, Any] = Field(default_factory=dict)


class LinkIn(BaseModel):
    src_key: str
    dst_key: str
    relation: str
    props: Dict[str, Any] = Field(default_factory=dict)


class EntitiesIn(BaseModel):
    entities: List[EntityIn] | None = None
    links: List[LinkIn] | None = None


class GraphBundle(BaseModel):
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]


class GlobeQuery(BaseModel):
    start: str
    end: str
    analytics: Literal["none", "simple", "advanced"] = "none"
    z_thr: float = 3.0
    contamination: float = 0.05
    speed_thr: float = 900.0
    dist_thr: float = 2000.0
    cluster: bool = False
    bucket: Optional[Literal["1m", "5m", "15m", "1h", "6h", "1d"]] = None
    tz: Optional[str] = None
    metric: Optional[Literal["count", "avg", "max", "sum"]] = "count"
    value_field: Optional[str] = None
    where: Optional[List[Dict[str, Any]]] = None
    # NEW:
    route_mode: bool = False
    route_metric: Optional[Literal["count", "avg", "max", "sum"]] = "count"
    carrier: Optional[str] = None
    source: Optional[str]


# --- The following schemas are included here to add the requested `z_thr` field ---
# They assume `VizCommonParams` and `Metric` exist elsewhere in this module/package.
# If they are defined in this file, the definitions below will pick up those types.

Metric = Literal["count", "avg", "max", "sum"]  # keep aligned with usage elsewhere


class VizCommonParams(BaseModel):
    # Placeholder for shared visualization parameters; keep consistent with your codebase
    start: Optional[str] = None
    end: Optional[str] = None
    bucket: Optional[Literal["1m", "5m", "15m", "1h", "6h", "1d"]] = None
    tz: Optional[str] = None
    where: Optional[List[Dict[str, Any]]] = None
    source: Optional[str] = None


class GridQuery(VizCommonParams):
    aggregate: bool = False
    group_by: List[str] = Field(default_factory=list)
    metric: Metric = "count"
    value_field: Optional[str] = None
    z_thr: float = 3.0
    source: Optional[str] = None


class GraphQuery(VizCommonParams):
    metric: Metric = "count"
    value_field: Optional[str] = None
    min_link_value: float = 1.0
    max_nodes: int = 4000
    max_links: int = 6000
    z_thr: float = 3.0
    source: Optional[str] = None