# path: domain/rules_models.py
"""
Database models for persistent rule storage.
"""

from __future__ import annotations
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, DateTime, Float, Boolean, JSON, Text

from domain.models import Base


class RuleDefinition(Base):
    """Stored rule definition."""
    __tablename__ = "rule_definitions"

    id: Mapped[str] = mapped_column(
        String(64), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    severity: Mapped[str] = mapped_column(String(16), default="medium")  # low/medium/high/critical
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    conditions: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    tags: Mapped[List[str]] = mapped_column(JSON, default=list)
    actions: Mapped[List[str]] = mapped_column(JSON, default=lambda: ["flag"])
    score_contribution: Mapped[float] = mapped_column(Float, default=0.0)
    metadata: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    # Audit fields
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )
    created_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for rules engine."""
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
            "metadata": self.metadata,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any], created_by: Optional[str] = None) -> "RuleDefinition":
        """Create from dictionary."""
        return RuleDefinition(
            id=data.get("id", str(uuid.uuid4())),
            name=data["name"],
            description=data.get("description", ""),
            severity=data.get("severity", "medium"),
            enabled=data.get("enabled", True),
            conditions=data["conditions"],
            tags=data.get("tags", []),
            actions=data.get("actions", ["flag"]),
            score_contribution=data.get("score_contribution", 0.0),
            metadata=data.get("metadata", {}),
            created_by=created_by,
        )


class RuleMatch(Base):
    """Record of a rule match for audit/investigation."""
    __tablename__ = "rule_matches"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    rule_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    rule_name: Mapped[str] = mapped_column(String(256), nullable=False)
    event_id: Mapped[Optional[str]] = mapped_column(String(36), index=True, nullable=True)
    user_id: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    matched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, index=True
    )
    match_details: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    # For investigation tracking
    reviewed: Mapped[bool] = mapped_column(Boolean, default=False)
    reviewed_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    disposition: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)  # confirmed_fraud/false_positive/etc
    notes: Mapped[str] = mapped_column(Text, default="")