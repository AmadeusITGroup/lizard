# path: domain/models.py
from __future__ import annotations
import uuid
from typing import Any, Dict, List, Optional
from datetime import datetime

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, DateTime, Float, Boolean, JSON, ForeignKey, Text, Integer, Index, UniqueConstraint
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncEngine


class Base(DeclarativeBase):
    pass


def make_engine_and_session(db_url: str):
    """Create async engine and session maker for the given database URL."""
    eng: AsyncEngine = create_async_engine(db_url, echo=False, future=True)
    session = async_sessionmaker(eng, expire_on_commit=False)
    return eng, session


# ============================================================
# Core Event Model
# ============================================================

class Event(Base):
    __tablename__ = "events"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    source: Mapped[str] = mapped_column(String(64), index=True)
    event_type: Mapped[str] = mapped_column(String(64), index=True)
    user_id: Mapped[Optional[str]] = mapped_column(String(128), index=True)
    account_id: Mapped[Optional[str]] = mapped_column(String(128), index=True)
    device_id: Mapped[Optional[str]] = mapped_column(String(128), index=True)
    card_hash: Mapped[Optional[str]] = mapped_column(String(128), index=True)
    ip: Mapped[Optional[str]] = mapped_column(String(64), index=True)
    geo_lat: Mapped[Optional[float]] = mapped_column(Float)
    geo_lon: Mapped[Optional[float]] = mapped_column(Float)
    country: Mapped[Optional[str]] = mapped_column(String(64))
    city: Mapped[Optional[str]] = mapped_column(String(64))
    is_unusual: Mapped[bool] = mapped_column(Boolean, default=False)
    # NEW:  Ticket-specific fields (all optional)
    office_id: Mapped[Optional[str]] = mapped_column(String(64))
    user_sign: Mapped[Optional[str]] = mapped_column(String(64))
    organization: Mapped[Optional[str]] = mapped_column(String(128))
    pnr: Mapped[Optional[str]] = mapped_column(String(64), index=True)
    carrier: Mapped[Optional[str]] = mapped_column(String(16))
    origin: Mapped[Optional[str]] = mapped_column(String(16))
    dest: Mapped[Optional[str]] = mapped_column(String(16))
    tkt_number: Mapped[Optional[str]] = mapped_column(String(64), index=True)
    status: Mapped[Optional[str]] = mapped_column(String(32))
    pos_country: Mapped[Optional[str]] = mapped_column(String(16))
    card_country: Mapped[Optional[str]] = mapped_column(String(16))
    advance_hours: Mapped[Optional[float]] = mapped_column(Float)
    stay_nights: Mapped[Optional[int]] = mapped_column(Integer)
    amount: Mapped[Optional[float]] = mapped_column(Float)
    currency: Mapped[Optional[str]] = mapped_column(String(8))
    fop_type: Mapped[Optional[str]] = mapped_column(String(32))
    fop_name: Mapped[Optional[str]] = mapped_column(String(64))
    fop_subtype: Mapped[Optional[str]] = mapped_column(String(32))
    card_last4: Mapped[Optional[str]] = mapped_column(String(8))
    card_bin: Mapped[Optional[str]] = mapped_column(String(16))
    is_fraud_indicator: Mapped[Optional[bool]] = mapped_column(Boolean)
    failure_reason: Mapped[Optional[str]] = mapped_column(String(128))
    legs: Mapped[Optional[str]] = mapped_column(String(512))  # Store as JSON string

    meta: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    @staticmethod
    def from_in(item) -> "Event":
        return Event(
            ts=item.ts,
            source=item.source,
            event_type=item.event_type,
            user_id=item.user_id,
            account_id=item.account_id,
            device_id=item.device_id,
            card_hash=item.card_hash,
            ip=item.ip,
            geo_lat=item.geo_lat,
            geo_lon=item.geo_lon,
            country=item.country,
            city=item.city,
            is_unusual=item.is_unusual or False,

            # NEW: Ticket fields
            office_id=item.office_id,
            user_sign=item.user_sign,
            organization=item.organization,
            pnr=item.pnr,
            carrier=item.carrier,
            origin=item.origin,
            dest=item.dest,
            tkt_number=item.tkt_number,
            status=item.status,
            pos_country=item.pos_country,
            card_country=item.card_country,
            advance_hours=item.advance_hours,
            stay_nights=item.stay_nights,
            amount=item.amount,
            currency=item.currency,
            fop_type=item.fop_type,
            fop_name=item.fop_name,
            fop_subtype=item.fop_subtype,
            card_last4=item.card_last4,
            card_bin=item.card_bin,
            is_fraud_indicator=item.is_fraud_indicator,
            failure_reason=item.failure_reason,
            legs=item.legs,

            meta=item.meta or {},
        )

    def to_out(self):
        from domain.schemas import EventOut
        return EventOut(
            ts=self.ts,
            source=self.source,
            event_type=self.event_type,
            user_id=self.user_id,
            account_id=self.account_id,
            device_id=self.device_id,
            card_hash=self.card_hash,
            ip=self.ip,
            geo_lat=self.geo_lat,
            geo_lon=self.geo_lon,
            country=self.country,
            city=self.city,
            is_unusual=self.is_unusual,

            # NEW:  Ticket fields
            office_id=self.office_id,
            user_sign=self.user_sign,
            organization=self.organization,
            pnr=self.pnr,
            carrier=self.carrier,
            origin=self.origin,
            dest=self.dest,
            tkt_number=self.tkt_number,
            status=self.status,
            pos_country=self.pos_country,
            card_country=self.card_country,
            advance_hours=self.advance_hours,
            stay_nights=self.stay_nights,
            amount=self.amount,
            currency=self.currency,
            fop_type=self.fop_type,
            fop_name=self.fop_name,
            fop_subtype=self.fop_subtype,
            card_last4=self.card_last4,
            card_bin=self.card_bin,
            is_fraud_indicator=self.is_fraud_indicator,
            failure_reason=self.failure_reason,
            legs=self.legs,

            meta=self.meta,
        )

# ============================================================
# Entity & Link Models
# ============================================================

class Entity(Base):
    __tablename__ = "entities"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    type: Mapped[str] = mapped_column(String(16), index=True)  # USER|ACCOUNT|DEVICE|CARD|IP
    key: Mapped[str] = mapped_column(String(128), index=True, unique=False)
    props: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    def as_dict(self):
        return {"id": self.id, "type": self.type, "key": self.key, "props": self.props}


class Link(Base):
    __tablename__ = "links"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    src_entity_id: Mapped[str] = mapped_column(String(36), ForeignKey("entities.id", ondelete="CASCADE"))
    dst_entity_id: Mapped[str] = mapped_column(String(36), ForeignKey("entities.id", ondelete="CASCADE"))
    relation: Mapped[str] = mapped_column(String(32))
    props: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    def as_dict(self):
        return {"id": self.id, "src": self.src_entity_id, "dst": self.dst_entity_id, "relation": self.relation,
                "props": self.props}


# ============================================================
# Mapping Template Model
# ============================================================

class MappingTemplate(Base):
    """Reusable mapping template for data ingestion."""
    __tablename__ = "mapping_templates"

    id: Mapped[str] = mapped_column(
        String(64), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    description: Mapped[str] = mapped_column(Text, default="")

    # The actual mapping configuration
    mapping: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    # Expression definitions (__expr__ content)
    expressions: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    # Template metadata
    source_type: Mapped[str] = mapped_column(String(64), default="csv")
    category: Mapped[str] = mapped_column(String(64), default="general", index=True)
    tags: Mapped[List[str]] = mapped_column(JSON, default=list)

    # Sample columns this template was created from (for matching)
    sample_columns: Mapped[List[str]] = mapped_column(JSON, default=list)

    # Usage tracking
    use_count: Mapped[int] = mapped_column(Integer, default=0)
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Validation rules associated with this template
    validation_rules: Mapped[List[Dict[str, Any]]] = mapped_column(JSON, default=list)

    # Audit fields
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )
    created_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    is_builtin: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "mapping": self.mapping,
            "expressions": self.expressions,
            "source_type": self.source_type,
            "category": self.category,
            "tags": self.tags,
            "sample_columns": self.sample_columns,
            "validation_rules": self.validation_rules,
            "use_count": self.use_count,
            "last_used_at": self.last_used_at.isoformat() if self.last_used_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "is_builtin": self.is_builtin,
            "is_active": self.is_active,
        }


# ============================================================
# Ingestion Log Model
# ============================================================

class IngestionLog(Base):
    """Log of data ingestion operations for audit and debugging."""
    __tablename__ = "ingestion_logs"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )

    # Source info
    filename: Mapped[str] = mapped_column(String(512), nullable=False)
    source_name: Mapped[str] = mapped_column(String(256), nullable=False)
    file_size_bytes: Mapped[int] = mapped_column(Integer, default=0)

    # Template used (if any)
    template_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    template_name: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)

    # Mapping used
    mapping_used: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    # Results
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    rows_total: Mapped[int] = mapped_column(Integer, default=0)
    rows_ingested: Mapped[int] = mapped_column(Integer, default=0)
    rows_rejected: Mapped[int] = mapped_column(Integer, default=0)
    rows_warnings: Mapped[int] = mapped_column(Integer, default=0)

    # Validation results
    validation_errors: Mapped[List[Dict[str, Any]]] = mapped_column(JSON, default=list)
    validation_warnings: Mapped[List[Dict[str, Any]]] = mapped_column(JSON, default=list)

    # Timing
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    duration_ms: Mapped[int] = mapped_column(Integer, default=0)

    # Error details if failed
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("ix_ingestion_status_time", "status", "started_at"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "filename": self.filename,
            "source_name": self.source_name,
            "file_size_bytes": self.file_size_bytes,
            "template_id": self.template_id,
            "template_name": self.template_name,
            "status": self.status,
            "rows_total": self.rows_total,
            "rows_ingested": self.rows_ingested,
            "rows_rejected": self.rows_rejected,
            "rows_warnings": self.rows_warnings,
            "validation_errors": self.validation_errors[: 100],
            "validation_warnings": self.validation_warnings[:100],
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_ms": self.duration_ms,
            "error_message": self.error_message,
        }


# ============================================================
# Rule Definition Model (for Rules Engine)
# NOTE: 'metadata' is reserved in SQLAlchemy, renamed to 'rule_metadata'
# ============================================================

class RuleDefinition(Base):
    """Stored rule definition for fraud detection."""
    __tablename__ = "rule_definitions"

    id: Mapped[str] = mapped_column(
        String(64), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    severity: Mapped[str] = mapped_column(String(16), default="medium")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    conditions: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    tags: Mapped[List[str]] = mapped_column(JSON, default=list)
    actions: Mapped[List[str]] = mapped_column(JSON, default=list)
    score_contribution: Mapped[float] = mapped_column(Float, default=0.0)
    # RENAMED from 'metadata' to 'rule_metadata' to avoid SQLAlchemy conflict
    rule_metadata: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )
    created_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "severity": self.severity,
            "enabled": self.enabled,
            "conditions": self.conditions,
            "tags": self.tags,
            "actions": self.actions,
            "score_contribution": self.score_contribution,
            "metadata": self.rule_metadata,  # Return as 'metadata' in API
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


# ============================================================
# Rule Match Model (for audit trail)
# ============================================================

class RuleMatch(Base):
    """Record of a rule match for audit/investigation."""
    __tablename__ = "rule_matches"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    rule_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    rule_name: Mapped[str] = mapped_column(String(256), nullable=False)
    event_id: Mapped[Optional[str]] = mapped_column(String(36), index=True, nullable=True)
    user_id: Mapped[Optional[str]] = mapped_column(String(256), index=True, nullable=True)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    matched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, index=True
    )
    match_details: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    reviewed: Mapped[bool] = mapped_column(Boolean, default=False)
    reviewed_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    disposition: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    notes: Mapped[str] = mapped_column(Text, default="")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "event_id": self.event_id,
            "user_id": self.user_id,
            "severity": self.severity,
            "matched_at": self.matched_at.isoformat() if self.matched_at else None,
            "match_details": self.match_details,
            "reviewed": self.reviewed,
            "disposition": self.disposition,
            "notes": self.notes,
        }