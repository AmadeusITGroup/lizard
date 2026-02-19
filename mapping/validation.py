# path: mapping/validation.py
"""
Data validation engine for ingestion quality checks.
"""

from __future__ import annotations
import re
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import numpy as np


class RuleType(str, Enum):
    """Types of validation rules."""
    REQUIRED = "required"
    TYPE_CHECK = "type_check"
    RANGE = "range"
    REGEX = "regex"
    ENUM = "enum"
    LENGTH = "length"
    UNIQUE = "unique"
    DATE_FORMAT = "date_format"
    NOT_NULL = "not_null"
    NOT_EMPTY = "not_empty"
    CUSTOM = "custom"


class OnFailure(str, Enum):
    """Actions to take when validation fails."""
    REJECT = "reject"
    WARN = "warn"
    FIX = "fix"
    DEFAULT = "default"
    SKIP_FIELD = "skip_field"


class Severity(str, Enum):
    """Severity levels for validation issues."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


def _is_null(value: Any) -> bool:
    """Check if value is null."""
    if value is None:
        return True
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return True
    return False


def _is_empty(value: Any) -> bool:
    """Check if value is null or empty string."""
    if _is_null(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


@dataclass
class ValidationIssue:
    """A single validation issue found in the data."""
    row_index: int
    field: str
    rule_name: str
    rule_type: str
    severity: str
    message: str
    original_value: Any = None
    fixed_value: Any = None
    action_taken: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "row": self.row_index,
            "field": self.field,
            "rule": self.rule_name,
            "type": self.rule_type,
            "severity": self.severity,
            "message": self.message,
            "original": str(self.original_value)[:100] if self.original_value is not None else None,
            "fixed": str(self.fixed_value)[:100] if self.fixed_value is not None else None,
            "action": self.action_taken,
        }


@dataclass
class ValidationResult:
    """Result of validating a DataFrame."""
    is_valid: bool
    total_rows: int
    valid_rows: int
    rejected_rows: int
    warning_rows: int
    issues: List[ValidationIssue] = field(default_factory=list)
    valid_row_indices: List[int] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "total_rows": self.total_rows,
            "valid_rows": self.valid_rows,
            "rejected_rows": self.rejected_rows,
            "warning_rows": self.warning_rows,
            "issues": [i.to_dict() for i in self.issues[: 500]],
            "issues_truncated": len(self.issues) > 500,
            "issues_total": len(self.issues),
        }


@dataclass
class ValidationRule:
    """A validation rule definition."""
    name: str
    target_field: str
    rule_type: RuleType
    config: Dict[str, Any] = field(default_factory=dict)
    on_failure: OnFailure = OnFailure.WARN
    default_value: Any = None
    severity: Severity = Severity.WARNING
    enabled: bool = True

    def validate_value(self, value: Any, row_index: int) -> Optional[ValidationIssue]:
        """Validate a single value. Returns ValidationIssue if invalid, None if valid."""
        if not self.enabled:
            return None

        issue = self._check_value(value, row_index)
        if issue:
            issue.action_taken = self.on_failure.value
        return issue

    def _check_value(self, value: Any, row_index: int) -> Optional[ValidationIssue]:
        """Core validation logic by rule type."""
        is_null = _is_null(value)
        is_empty = _is_empty(value)

        # Required / Not Null checks
        if self.rule_type in (RuleType.REQUIRED, RuleType.NOT_NULL):
            if is_null:
                return ValidationIssue(
                    row_index=row_index,
                    field=self.target_field,
                    rule_name=self.name,
                    rule_type=self.rule_type.value,
                    severity=self.severity.value,
                    message=f"Required field '{self.target_field}' is null",
                    original_value=value,
                    fixed_value=self.default_value if self.on_failure == OnFailure.DEFAULT else None,
                )
            return None

        # Not Empty check
        if self.rule_type == RuleType.NOT_EMPTY:
            if is_empty:
                return ValidationIssue(
                    row_index=row_index,
                    field=self.target_field,
                    rule_name=self.name,
                    rule_type=self.rule_type.value,
                    severity=self.severity.value,
                    message=f"Field '{self.target_field}' is empty",
                    original_value=value,
                    fixed_value=self.default_value if self.on_failure == OnFailure.DEFAULT else None,
                )
            return None

        # Skip other checks if value is null
        if is_null:
            return None

        # Type check
        if self.rule_type == RuleType.TYPE_CHECK:
            expected_type = self.config.get("type", "string")
            if not self._check_type(value, expected_type):
                return ValidationIssue(
                    row_index=row_index,
                    field=self.target_field,
                    rule_name=self.name,
                    rule_type=self.rule_type.value,
                    severity=self.severity.value,
                    message=f"Field '{self.target_field}' expected type '{expected_type}', got '{type(value).__name__}'",
                    original_value=value,
                    fixed_value=self._try_convert(value, expected_type),
                )
            return None

        # Range check
        if self.rule_type == RuleType.RANGE:
            min_val = self.config.get("min")
            max_val = self.config.get("max")
            try:
                num_val = float(value)
                if min_val is not None and num_val < min_val:
                    return ValidationIssue(
                        row_index=row_index,
                        field=self.target_field,
                        rule_name=self.name,
                        rule_type=self.rule_type.value,
                        severity=self.severity.value,
                        message=f"Field '{self.target_field}' value {num_val} below minimum {min_val}",
                        original_value=value,
                        fixed_value=min_val if self.on_failure == OnFailure.FIX else None,
                    )
                if max_val is not None and num_val > max_val:
                    return ValidationIssue(
                        row_index=row_index,
                        field=self.target_field,
                        rule_name=self.name,
                        rule_type=self.rule_type.value,
                        severity=self.severity.value,
                        message=f"Field '{self.target_field}' value {num_val} above maximum {max_val}",
                        original_value=value,
                        fixed_value=max_val if self.on_failure == OnFailure.FIX else None,
                    )
            except (ValueError, TypeError):
                return ValidationIssue(
                    row_index=row_index,
                    field=self.target_field,
                    rule_name=self.name,
                    rule_type=self.rule_type.value,
                    severity=self.severity.value,
                    message=f"Field '{self.target_field}' cannot be converted to number for range check",
                    original_value=value,
                )
            return None

        # Regex check
        if self.rule_type == RuleType.REGEX:
            pattern = self.config.get("pattern", ".*")
            try:
                if not re.match(pattern, str(value)):
                    return ValidationIssue(
                        row_index=row_index,
                        field=self.target_field,
                        rule_name=self.name,
                        rule_type=self.rule_type.value,
                        severity=self.severity.value,
                        message=f"Field '{self.target_field}' does not match pattern '{pattern}'",
                        original_value=value,
                    )
            except re.error as e:
                return ValidationIssue(
                    row_index=row_index,
                    field=self.target_field,
                    rule_name=self.name,
                    rule_type=self.rule_type.value,
                    severity=self.severity.value,
                    message=f"Invalid regex pattern: {e}",
                    original_value=value,
                )
            return None

        # Enum check
        if self.rule_type == RuleType.ENUM:
            allowed = self.config.get("values", [])
            str_allowed = [str(v) for v in allowed]
            if str(value) not in str_allowed:
                return ValidationIssue(
                    row_index=row_index,
                    field=self.target_field,
                    rule_name=self.name,
                    rule_type=self.rule_type.value,
                    severity=self.severity.value,
                    message=f"Field '{self.target_field}' value '{value}' not in allowed values:  {allowed[: 10]}",
                    original_value=value,
                    fixed_value=self.default_value if self.on_failure == OnFailure.DEFAULT else None,
                )
            return None

        # Length check
        if self.rule_type == RuleType.LENGTH:
            min_len = self.config.get("min", 0)
            max_len = self.config.get("max")
            str_val = str(value)
            if len(str_val) < min_len:
                return ValidationIssue(
                    row_index=row_index,
                    field=self.target_field,
                    rule_name=self.name,
                    rule_type=self.rule_type.value,
                    severity=self.severity.value,
                    message=f"Field '{self.target_field}' length {len(str_val)} below minimum {min_len}",
                    original_value=value,
                )
            if max_len is not None and len(str_val) > max_len:
                return ValidationIssue(
                    row_index=row_index,
                    field=self.target_field,
                    rule_name=self.name,
                    rule_type=self.rule_type.value,
                    severity=self.severity.value,
                    message=f"Field '{self.target_field}' length {len(str_val)} above maximum {max_len}",
                    original_value=value,
                    fixed_value=str_val[:int(max_len)] if self.on_failure == OnFailure.FIX else None,
                )
            return None

        # Date format check
        if self.rule_type == RuleType.DATE_FORMAT:
            fmt = self.config.get("format", "%Y-%m-%d")
            try:
                datetime.strptime(str(value), fmt)
            except ValueError:
                return ValidationIssue(
                    row_index=row_index,
                    field=self.target_field,
                    rule_name=self.name,
                    rule_type=self.rule_type.value,
                    severity=self.severity.value,
                    message=f"Field '{self.target_field}' does not match date format '{fmt}'",
                    original_value=value,
                )
            return None

        return None

    def _check_type(self, value: Any, expected: str) -> bool:
        """Check if value matches expected type."""
        if expected in ("string", "str"):
            return isinstance(value, str)
        if expected in ("number", "float", "numeric"):
            try:
                float(value)
                return True
            except (ValueError, TypeError):
                return False
        if expected in ("integer", "int"):
            try:
                f = float(value)
                return f == int(f)
            except (ValueError, TypeError):
                return False
        if expected in ("boolean", "bool"):
            return isinstance(value, bool) or str(value).lower() in ("true", "false", "1", "0", "yes", "no")
        return True

    def _try_convert(self, value: Any, expected: str) -> Any:
        """Try to convert value to expected type."""
        try:
            if expected in ("string", "str"):
                return str(value)
            if expected in ("number", "float", "numeric"):
                return float(str(value).replace(",", "."))
            if expected in ("integer", "int"):
                return int(float(str(value).replace(",", ".")))
            if expected in ("boolean", "bool"):
                return str(value).lower() in ("true", "1", "yes")
        except (ValueError, TypeError):
            pass
        return None


class DataValidator:
    """Validates a DataFrame against a set of rules."""

    def __init__(self, rules: Optional[List[Dict[str, Any]]] = None):
        self.rules: List[ValidationRule] = []
        if rules:
            for r in rules:
                self.add_rule(r)

    def add_rule(self, rule_dict: Dict[str, Any]) -> ValidationRule:
        """Add a rule from dictionary definition."""
        rule = ValidationRule(
            name=rule_dict.get("name", "Unnamed Rule"),
            target_field=rule_dict["target_field"],
            rule_type=RuleType(rule_dict["rule_type"]),
            config=rule_dict.get("config", {}),
            on_failure=OnFailure(rule_dict.get("on_failure", "warn")),
            default_value=rule_dict.get("default_value"),
            severity=Severity(rule_dict.get("severity", "warning")),
            enabled=rule_dict.get("enabled", True),
        )
        self.rules.append(rule)
        return rule

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, ValidationResult]:
        """
        Validate a DataFrame and return the cleaned DataFrame + validation result.

        Returns:
            - Cleaned DataFrame (with rejected rows removed, fixes applied)
            - ValidationResult with details
        """
        if df.empty:
            return df, ValidationResult(
                is_valid=True,
                total_rows=0,
                valid_rows=0,
                rejected_rows=0,
                warning_rows=0,
                valid_row_indices=[],
            )

        all_issues: List[ValidationIssue] = []
        rejected_rows: set = set()
        warning_rows: set = set()

        # Create a copy to modify
        result_df = df.copy()

        # Validate each rule
        for rule in self.rules:
            if not rule.enabled:
                continue

            if rule.target_field not in result_df.columns:
                # Field doesn't exist - issue warning for required fields
                if rule.rule_type in (RuleType.REQUIRED, RuleType.NOT_NULL):
                    for idx in range(len(result_df)):
                        issue = ValidationIssue(
                            row_index=idx,
                            field=rule.target_field,
                            rule_name=rule.name,
                            rule_type=rule.rule_type.value,
                            severity=rule.severity.value,
                            message=f"Required field '{rule.target_field}' does not exist in data",
                            action_taken="rejected" if rule.on_failure == OnFailure.REJECT else "warned",
                        )
                        all_issues.append(issue)
                        if rule.on_failure == OnFailure.REJECT:
                            rejected_rows.add(idx)
                        else:
                            warning_rows.add(idx)
                continue

            # Validate each row
            for idx in range(len(result_df)):
                value = result_df.iloc[idx][rule.target_field]
                issue = rule.validate_value(value, idx)

                if issue:
                    all_issues.append(issue)

                    if rule.on_failure == OnFailure.REJECT:
                        rejected_rows.add(idx)
                    elif rule.on_failure == OnFailure.FIX and issue.fixed_value is not None:
                        result_df.iat[idx, result_df.columns.get_loc(rule.target_field)] = issue.fixed_value
                        warning_rows.add(idx)
                    elif rule.on_failure == OnFailure.DEFAULT and rule.default_value is not None:
                        result_df.iat[idx, result_df.columns.get_loc(rule.target_field)] = rule.default_value
                        warning_rows.add(idx)
                    elif rule.on_failure == OnFailure.SKIP_FIELD:
                        result_df.iat[idx, result_df.columns.get_loc(rule.target_field)] = None
                        warning_rows.add(idx)
                    else:
                        warning_rows.add(idx)

        # Remove rejected rows
        valid_indices = [i for i in range(len(result_df)) if i not in rejected_rows]
        result_df = result_df.iloc[valid_indices].reset_index(drop=True)

        # Calculate stats
        total_rows = len(df)
        valid_rows = len(result_df)
        rejected_count = len(rejected_rows)
        warning_count = len(warning_rows - rejected_rows)

        return result_df, ValidationResult(
            is_valid=rejected_count == 0,
            total_rows=total_rows,
            valid_rows=valid_rows,
            rejected_rows=rejected_count,
            warning_rows=warning_count,
            issues=all_issues,
            valid_row_indices=valid_indices,
        )

    def validate_row(self, row: Dict[str, Any], row_index: int = 0) -> List[ValidationIssue]:
        """Validate a single row (dict) against all rules."""
        issues = []
        for rule in self.rules:
            if not rule.enabled:
                continue
            value = row.get(rule.target_field)
            issue = rule.validate_value(value, row_index)
            if issue:
                issues.append(issue)
        return issues


# ============================================================
# Built-in validation rules
# ============================================================

BUILTIN_VALIDATION_RULES: List[Dict[str, Any]] = [
    {
        "name": "Timestamp Required",
        "target_field": "ts",
        "rule_type": "required",
        "on_failure": "reject",
        "severity": "error",
        "config": {},
    },
    {
        "name": "User ID Not Empty",
        "target_field": "user_id",
        "rule_type": "not_empty",
        "on_failure": "default",
        "default_value": "unknown",
        "severity": "warning",
        "config": {},
    },
    {
        "name": "Amount Non-Negative",
        "target_field": "amount",
        "rule_type": "range",
        "on_failure": "fix",
        "severity": "warning",
        "config": {"min": 0},
    },
    {
        "name": "Latitude Valid Range",
        "target_field": "geo_lat",
        "rule_type": "range",
        "on_failure": "warn",
        "severity": "warning",
        "config": {"min": -90, "max": 90},
    },
    {
        "name": "Longitude Valid Range",
        "target_field": "geo_lon",
        "rule_type": "range",
        "on_failure": "warn",
        "severity": "warning",
        "config": {"min": -180, "max": 180},
    },
    {
        "name": "IP Address Format",
        "target_field": "ip",
        "rule_type": "regex",
        "on_failure": "warn",
        "severity": "info",
        "config": {"pattern": r"^(\d{1,3}\.){3}\d{1,3}$|^([0-9a-fA-F: ]+)$"},
    },
    {
        "name": "Country Code Length",
        "target_field": "country",
        "rule_type": "length",
        "on_failure": "warn",
        "severity": "info",
        "config": {"min": 2, "max": 3},
    },
    {
        "name": "Currency Code Format",
        "target_field": "currency",
        "rule_type": "regex",
        "on_failure": "warn",
        "severity": "info",
        "config": {"pattern": r"^[A-Z]{3}$"},
    },
    {
        "name": "Event Type Not Empty",
        "target_field": "event_type",
        "rule_type": "not_empty",
        "on_failure": "default",
        "default_value": "unknown",
        "severity": "warning",
        "config": {},
    },
    {
        "name": "PNR Format",
        "target_field": "pnr",
        "rule_type": "regex",
        "on_failure": "warn",
        "severity": "info",
        "config": {"pattern": r"^[A-Z0-9]{5,8}$"},
    },
    {
        "name": "Carrier Code Format",
        "target_field": "carrier",
        "rule_type": "regex",
        "on_failure": "warn",
        "severity": "info",
        "config": {"pattern": r"^[A-Z0-9]{2,3}$"},
    },
    {
        "name": "Airport Code Format (Origin)",
        "target_field": "origin",
        "rule_type": "regex",
        "on_failure": "warn",
        "severity": "info",
        "config": {"pattern": r"^[A-Z]{3}$"},
    },
    {
        "name": "Airport Code Format (Dest)",
        "target_field": "dest",
        "rule_type": "regex",
        "on_failure": "warn",
        "severity": "info",
        "config": {"pattern": r"^[A-Z]{3}$"},
    },
]


def get_builtin_rules() -> List[Dict[str, Any]]:
    """Get list of built-in validation rules."""
    return BUILTIN_VALIDATION_RULES.copy()


def create_validator_from_template(template: Dict[str, Any]) -> DataValidator:
    """Create a DataValidator from a mapping template."""
    rules = template.get("validation_rules", [])
    return DataValidator(rules)