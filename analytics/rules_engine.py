# path: analytics/rules_engine.py
"""
LIZARD Rules Engine - Define custom fraud detection rules.

Rules are defined as JSON/dict structures that can be:
- Stored in database for persistence
- Defined per investigation/scenario
- Combined with ML-based anomaly detection

Example rule: 
{
    "id": "velocity_new_device",
    "name": "High velocity with new device",
    "description": "Flag when travel speed exceeds threshold AND device is new",
    "severity": "high",
    "enabled": True,
    "conditions": {
        "operator": "AND",
        "rules": [
            {"field": "speed_kmh", "op": "gt", "value": 500},
            {"field": "is_new_device", "op": "eq", "value": 1}
        ]
    },
    "actions": ["flag", "alert"],
    "tags": ["velocity", "device", "ato"]
}
"""

from __future__ import annotations
import re
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import numpy as np


class Severity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Operator(str, Enum):
    # Comparison
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    # String
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTSWITH = "startswith"
    ENDSWITH = "endswith"
    MATCHES = "matches"  # regex
    # List
    IN = "in"
    NOT_IN = "not_in"
    # Null checks
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    # Range
    BETWEEN = "between"
    NOT_BETWEEN = "not_between"


class LogicalOperator(str, Enum):
    AND = "AND"
    OR = "OR"
    NOT = "NOT"


@dataclass
class Condition:
    """A single condition to evaluate."""
    field: str
    op: Operator
    value: Any = None
    value2: Any = None  # For BETWEEN operator

    def evaluate(self, row: Dict[str, Any]) -> bool:
        """Evaluate this condition against a row of data."""
        field_value = self._get_field_value(row, self.field)

        # Handle null checks first
        if self.op == Operator.IS_NULL:
            return self._is_null(field_value)
        if self.op == Operator.IS_NOT_NULL:
            return not self._is_null(field_value)

        # If field is null and we're doing comparison, return False
        if self._is_null(field_value):
            return False

        # Comparison operators
        if self.op == Operator.EQ:
            return field_value == self.value
        if self.op == Operator.NE:
            return field_value != self.value
        if self.op == Operator.GT:
            return self._compare_numeric(field_value, self.value, lambda a, b: a > b)
        if self.op == Operator.GTE:
            return self._compare_numeric(field_value, self.value, lambda a, b: a >= b)
        if self.op == Operator.LT:
            return self._compare_numeric(field_value, self.value, lambda a, b: a < b)
        if self.op == Operator.LTE:
            return self._compare_numeric(field_value, self.value, lambda a, b: a <= b)

        # String operators
        if self.op == Operator.CONTAINS:
            return str(self.value).lower() in str(field_value).lower()
        if self.op == Operator.NOT_CONTAINS:
            return str(self.value).lower() not in str(field_value).lower()
        if self.op == Operator.STARTSWITH:
            return str(field_value).lower().startswith(str(self.value).lower())
        if self.op == Operator.ENDSWITH:
            return str(field_value).lower().endswith(str(self.value).lower())
        if self.op == Operator.MATCHES:
            try:
                return bool(re.search(str(self.value), str(field_value), re.IGNORECASE))
            except re.error:
                return False

        # List operators
        if self.op == Operator.IN:
            values = self.value if isinstance(self.value, (list, tuple, set)) else [self.value]
            return field_value in values
        if self.op == Operator.NOT_IN:
            values = self.value if isinstance(self.value, (list, tuple, set)) else [self.value]
            return field_value not in values

        # Range operators
        if self.op == Operator.BETWEEN:
            return self._compare_numeric(field_value, self.value, lambda a, b: a >= b) and \
                self._compare_numeric(field_value, self.value2, lambda a, b: a <= b)
        if self.op == Operator.NOT_BETWEEN:
            return not (self._compare_numeric(field_value, self.value, lambda a, b: a >= b) and \
                        self._compare_numeric(field_value, self.value2, lambda a, b: a <= b))

        return False

    def _get_field_value(self, row: Dict[str, Any], field: str) -> Any:
        """Get field value, supporting nested fields with dot notation."""
        if "." in field:
            parts = field.split(".")
            value = row
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    return None
            return value
        return row.get(field)

    def _is_null(self, value: Any) -> bool:
        """Check if value is null/empty."""
        if value is None:
            return True
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return True
        if isinstance(value, str) and value.strip() == "":
            return True
        return False

    def _compare_numeric(self, a: Any, b: Any, comparator: Callable) -> bool:
        """Safely compare numeric values."""
        try:
            a_num = float(a) if not isinstance(a, (int, float)) else a
            b_num = float(b) if not isinstance(b, (int, float)) else b
            if math.isnan(a_num) or math.isnan(b_num):
                return False
            return comparator(a_num, b_num)
        except (ValueError, TypeError):
            # Fall back to string comparison
            return comparator(str(a), str(b))


@dataclass
class ConditionGroup:
    """A group of conditions combined with a logical operator."""
    operator: LogicalOperator
    conditions: List[Union[Condition, "ConditionGroup"]] = field(default_factory=list)

    def evaluate(self, row: Dict[str, Any]) -> bool:
        """Evaluate all conditions in this group."""
        if not self.conditions:
            return True

        if self.operator == LogicalOperator.NOT:
            # NOT applies to first condition only
            if self.conditions:
                return not self._evaluate_single(self.conditions[0], row)
            return True

        results = [self._evaluate_single(c, row) for c in self.conditions]

        if self.operator == LogicalOperator.AND:
            return all(results)
        if self.operator == LogicalOperator.OR:
            return any(results)

        return False

    def _evaluate_single(self, condition: Union[Condition, "ConditionGroup"], row: Dict[str, Any]) -> bool:
        """Evaluate a single condition or nested group."""
        if isinstance(condition, ConditionGroup):
            return condition.evaluate(row)
        return condition.evaluate(row)


@dataclass
class Rule:
    """A complete fraud detection rule."""
    id: str
    name: str
    conditions: ConditionGroup
    severity: Severity = Severity.MEDIUM
    description: str = ""
    enabled: bool = True
    tags: List[str] = field(default_factory=list)
    actions: List[str] = field(default_factory=lambda: ["flag"])
    score_contribution: float = 0.0  # Added to anomaly score when triggered
    metadata: Dict[str, Any] = field(default_factory=dict)

    def evaluate(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Evaluate rule against a row.
        Returns match details if triggered, None otherwise.
        """
        if not self.enabled:
            return None

        if self.conditions.evaluate(row):
            return {
                "rule_id": self.id,
                "rule_name": self.name,
                "severity": self.severity.value,
                "description": self.description,
                "tags": self.tags,
                "actions": self.actions,
                "score_contribution": self.score_contribution,
            }
        return None


def parse_condition(cond_dict: Dict[str, Any]) -> Union[Condition, ConditionGroup]:
    """Parse a condition dictionary into Condition or ConditionGroup."""
    # Check if it's a group (has 'operator' and 'rules')
    if "rules" in cond_dict or "conditions" in cond_dict:
        operator = LogicalOperator(cond_dict.get("operator", "AND").upper())
        rules = cond_dict.get("rules") or cond_dict.get("conditions", [])
        conditions = [parse_condition(r) for r in rules]
        return ConditionGroup(operator=operator, conditions=conditions)

    # It's a single condition
    return Condition(
        field=cond_dict["field"],
        op=Operator(cond_dict["op"]),
        value=cond_dict.get("value"),
        value2=cond_dict.get("value2"),
    )


def parse_rule(rule_dict: Dict[str, Any]) -> Rule:
    """Parse a rule dictionary into a Rule object."""
    conditions = parse_condition(rule_dict.get("conditions", {}))
    if isinstance(conditions, Condition):
        # Wrap single condition in a group
        conditions = ConditionGroup(operator=LogicalOperator.AND, conditions=[conditions])

    return Rule(
        id=rule_dict["id"],
        name=rule_dict["name"],
        conditions=conditions,
        severity=Severity(rule_dict.get("severity", "medium").lower()),
        description=rule_dict.get("description", ""),
        enabled=rule_dict.get("enabled", True),
        tags=rule_dict.get("tags", []),
        actions=rule_dict.get("actions", ["flag"]),
        score_contribution=float(rule_dict.get("score_contribution", 0.0)),
        metadata=rule_dict.get("metadata", {}),
    )


class RulesEngine:
    """
    Engine for evaluating multiple fraud detection rules against events.
    """

    def __init__(self, rules: Optional[List[Dict[str, Any]]] = None):
        self.rules: List[Rule] = []
        if rules:
            for r in rules:
                self.add_rule(r)

    def add_rule(self, rule_dict: Dict[str, Any]) -> Rule:
        """Add a rule from dictionary definition."""
        rule = parse_rule(rule_dict)
        self.rules.append(rule)
        return rule

    def remove_rule(self, rule_id: str) -> bool:
        """Remove a rule by ID."""
        initial_len = len(self.rules)
        self.rules = [r for r in self.rules if r.id != rule_id]
        return len(self.rules) < initial_len

    def get_rule(self, rule_id: str) -> Optional[Rule]:
        """Get a rule by ID."""
        for r in self.rules:
            if r.id == rule_id:
                return r
        return None

    def enable_rule(self, rule_id: str) -> bool:
        """Enable a rule."""
        rule = self.get_rule(rule_id)
        if rule:
            rule.enabled = True
            return True
        return False

    def disable_rule(self, rule_id: str) -> bool:
        """Disable a rule."""
        rule = self.get_rule(rule_id)
        if rule:
            rule.enabled = False
            return True
        return False

    def evaluate_row(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Evaluate all rules against a single row."""
        matches = []
        for rule in self.rules:
            result = rule.evaluate(row)
            if result:
                matches.append(result)
        return matches

    def evaluate_dataframe(
            self,
            df: pd.DataFrame,
            add_columns: bool = True,
    ) -> pd.DataFrame:
        """
        Evaluate all rules against a DataFrame.

        If add_columns=True, adds:
        - rule_matches: list of matched rule details
        - rule_ids: list of matched rule IDs
        - rule_severity: highest severity among matches
        - rule_score: sum of score contributions
        - rule_tags: combined tags from all matches
        """
        if df.empty:
            if add_columns:
                df = df.copy()
                df["rule_matches"] = []
                df["rule_ids"] = []
                df["rule_severity"] = None
                df["rule_score"] = 0.0
                df["rule_tags"] = []
            return df

        result = df.copy()

        # Evaluate each row
        matches_list = []
        for _, row in result.iterrows():
            row_dict = row.to_dict()
            matches = self.evaluate_row(row_dict)
            matches_list.append(matches)

        if add_columns:
            result["rule_matches"] = matches_list
            result["rule_ids"] = [
                [m["rule_id"] for m in matches] for matches in matches_list
            ]

            # Calculate highest severity
            severity_order = {"critical": 4, "high": 3, "medium": 2, "low": 1}

            def get_max_severity(matches):
                if not matches:
                    return None
                severities = [m["severity"] for m in matches]
                return max(severities, key=lambda s: severity_order.get(s, 0))

            result["rule_severity"] = [get_max_severity(m) for m in matches_list]

            # Sum score contributions
            result["rule_score"] = [
                sum(m.get("score_contribution", 0) for m in matches)
                for matches in matches_list
            ]

            # Combine all tags
            result["rule_tags"] = [
                list(set(tag for m in matches for tag in m.get("tags", [])))
                for matches in matches_list
            ]

        return result

    def get_rules_summary(self) -> List[Dict[str, Any]]:
        """Get summary of all rules."""
        return [
            {
                "id": r.id,
                "name": r.name,
                "severity": r.severity.value,
                "enabled": r.enabled,
                "tags": r.tags,
                "description": r.description,
            }
            for r in self.rules
        ]

    def export_rules(self) -> List[Dict[str, Any]]:
        """Export all rules as dictionaries for storage."""

        def condition_to_dict(c: Union[Condition, ConditionGroup]) -> Dict[str, Any]:
            if isinstance(c, ConditionGroup):
                return {
                    "operator": c.operator.value,
                    "rules": [condition_to_dict(sub) for sub in c.conditions],
                }
            return {
                "field": c.field,
                "op": c.op.value,
                "value": c.value,
                **({"value2": c.value2} if c.value2 is not None else {}),
            }

        return [
            {
                "id": r.id,
                "name": r.name,
                "description": r.description,
                "severity": r.severity.value,
                "enabled": r.enabled,
                "conditions": condition_to_dict(r.conditions),
                "tags": r.tags,
                "actions": r.actions,
                "score_contribution": r.score_contribution,
                "metadata": r.metadata,
            }
            for r in self.rules
        ]


# ============================================================
# Pre-built rule templates for common fraud patterns
# ============================================================

BUILTIN_RULES: List[Dict[str, Any]] = [
    {
        "id": "impossible_travel",
        "name": "Impossible Travel",
        "description": "Travel speed exceeds physically possible limits (>900 km/h)",
        "severity": "high",
        "enabled": True,
        "conditions": {
            "operator": "AND",
            "rules": [
                {"field": "speed_kmh", "op": "gt", "value": 900},
                {"field": "dist_prev_km", "op": "gt", "value": 100},
            ]
        },
        "tags": ["velocity", "geo", "ato"],
        "score_contribution": 0.3,
    },
    {
        "id": "new_device_high_value",
        "name": "New Device High Value Transaction",
        "description": "First-time device used for high-value transaction",
        "severity": "high",
        "enabled": True,
        "conditions": {
            "operator": "AND",
            "rules": [
                {"field": "is_new_device", "op": "eq", "value": 1},
                {"field": "amount", "op": "gt", "value": 1000},
            ]
        },
        "tags": ["device", "amount", "ato"],
        "score_contribution": 0.25,
    },
    {
        "id": "rapid_failures",
        "name": "Rapid Authentication Failures",
        "description": "Multiple failed authentication attempts detected",
        "severity": "medium",
        "enabled": True,
        "conditions": {
            "operator": "AND",
            "rules": [
                {"field": "event_type", "op": "contains", "value": "fail"},
                {"field": "z_fail", "op": "gt", "value": 2.5},
            ]
        },
        "tags": ["auth", "brute_force"],
        "score_contribution": 0.2,
    },
    {
        "id": "far_from_home",
        "name": "Far From Home Location",
        "description": "Activity from location far from user's typical location",
        "severity": "medium",
        "enabled": True,
        "conditions": {
            "operator": "AND",
            "rules": [
                {"field": "dist_home_km", "op": "gt", "value": 2000},
            ]
        },
        "tags": ["geo", "location"],
        "score_contribution": 0.15,
    },
    {
        "id": "new_ip_new_device",
        "name": "New IP and New Device Combination",
        "description": "Both IP and device are new for this user",
        "severity": "high",
        "enabled": True,
        "conditions": {
            "operator": "AND",
            "rules": [
                {"field": "is_new_ip", "op": "eq", "value": 1},
                {"field": "is_new_device", "op": "eq", "value": 1},
            ]
        },
        "tags": ["device", "ip", "ato"],
        "score_contribution": 0.25,
    },
    {
        "id": "unusual_hour",
        "name": "Unusual Activity Hour",
        "description": "Activity during rare hours for this user",
        "severity": "low",
        "enabled": True,
        "conditions": {
            "operator": "AND",
            "rules": [
                {"field": "hour_rarity", "op": "gt", "value": 3.0},
            ]
        },
        "tags": ["temporal", "behavior"],
        "score_contribution": 0.1,
    },
    {
        "id": "high_anomaly_score",
        "name": "High ML Anomaly Score",
        "description": "Machine learning model flagged as highly anomalous",
        "severity": "medium",
        "enabled": True,
        "conditions": {
            "operator": "AND",
            "rules": [
                {"field": "anom_score", "op": "gte", "value": 0.8},
            ]
        },
        "tags": ["ml", "anomaly"],
        "score_contribution": 0.2,
    },
    {
        "id": "country_mismatch",
        "name": "Country Mismatch",
        "description": "Card country differs from transaction country",
        "severity": "medium",
        "enabled": True,
        "conditions": {
            "operator": "AND",
            "rules": [
                {"field": "card_country", "op": "is_not_null", "value": None},
                {"field": "country", "op": "is_not_null", "value": None},
                {"field": "card_country", "op": "ne", "value": "${country}"},
            ]
        },
        "tags": ["geo", "card", "payment"],
        "score_contribution": 0.15,
    },
]


def create_default_engine() -> RulesEngine:
    """Create a rules engine with built-in rules."""
    engine = RulesEngine()
    for rule in BUILTIN_RULES:
        engine.add_rule(rule)
    return engine