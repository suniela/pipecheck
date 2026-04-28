"""Validates a pandas DataFrame against a PipelineSchema contract."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import pandas as pd

from pipecheck.schema import PipelineSchema

# Maps schema dtype strings to sets of acceptable pandas dtype kinds
_DTYPE_KIND_MAP: dict[str, set[str]] = {
    "int": {"i", "u"},
    "float": {"f"},
    "str": {"O", "S", "U"},
    "bool": {"b"},
    "datetime": {"M"},
}


@dataclass
class ValidationError:
    column: str
    rule: str
    message: str


@dataclass
class ValidationResult:
    passed: bool
    errors: List[ValidationError] = field(default_factory=list)

    def summary(self) -> str:
        if self.passed:
            return "Validation passed with no errors."
        lines = [f"Validation failed with {len(self.errors)} error(s):"]
        for err in self.errors:
            lines.append(f"  [{err.column}] {err.rule}: {err.message}")
        return "\n".join(lines)


def validate(df: pd.DataFrame, schema: PipelineSchema) -> ValidationResult:
    """Validate *df* against *schema* and return a :class:`ValidationResult`."""
    errors: list[ValidationError] = []

    # 1. Check required columns are present
    df_columns = set(df.columns)
    for col in schema.columns:
        if col.name not in df_columns:
            errors.append(
                ValidationError(
                    column=col.name,
                    rule="missing_column",
                    message=f"Column '{col.name}' is required but not found in DataFrame.",
                )
            )
            continue  # Skip further checks for missing columns

        series = df[col.name]

        # 2. dtype check
        kind = series.dtype.kind
        # str columns may also appear as object dtype
        accepted_kinds = _DTYPE_KIND_MAP.get(col.dtype, set())
        if col.dtype == "str":
            accepted_kinds = {"O", "S", "U"}
        if accepted_kinds and kind not in accepted_kinds:
            errors.append(
                ValidationError(
                    column=col.name,
                    rule="dtype_mismatch",
                    message=(
                        f"Expected dtype '{col.dtype}' (kind {accepted_kinds}), "
                        f"got numpy kind '{kind}'."
                    ),
                )
            )

        # 3. nullable check
        if not col.nullable and series.isna().any():
            null_count = int(series.isna().sum())
            errors.append(
                ValidationError(
                    column=col.name,
                    rule="null_violation",
                    message=f"Column is not nullable but contains {null_count} null value(s).",
                )
            )

        # 4. unique check
        if col.unique and series.duplicated().any():
            dup_count = int(series.duplicated().sum())
            errors.append(
                ValidationError(
                    column=col.name,
                    rule="unique_violation",
                    message=f"Column must be unique but contains {dup_count} duplicate value(s).",
                )
            )

    return ValidationResult(passed=len(errors) == 0, errors=errors)
