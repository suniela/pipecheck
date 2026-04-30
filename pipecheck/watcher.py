"""Schema drift watcher: compares a DataFrame's inferred schema against a PipelineSchema contract."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

import pandas as pd

from pipecheck.schema import PipelineSchema

# Mapping from pandas dtype kinds to the dtype strings used in ColumnSchema
_DTYPE_MAP = {
    "i": "int",
    "u": "int",
    "f": "float",
    "O": "str",
    "U": "str",
    "S": "str",
    "b": "bool",
    "M": "datetime",
    "m": "datetime",
}


def _infer_dtype(series: pd.Series) -> str:
    """Return a normalised dtype string for a pandas Series."""
    kind = series.dtype.kind
    return _DTYPE_MAP.get(kind, "str")


@dataclass
class DriftReport:
    """Result of a schema drift check."""

    missing_columns: List[str] = field(default_factory=list)
    extra_columns: List[str] = field(default_factory=list)
    type_mismatches: List[str] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return bool(self.missing_columns or self.extra_columns or self.type_mismatches)

    def summary(self) -> str:
        if not self.has_drift:
            return "No schema drift detected."
        lines = ["Schema drift detected:"]
        for col in self.missing_columns:
            lines.append(f"  [MISSING]   column '{col}' expected but not found in DataFrame")
        for col in self.extra_columns:
            lines.append(f"  [EXTRA]     column '{col}' found in DataFrame but not in schema")
        for msg in self.type_mismatches:
            lines.append(f"  [TYPE]      {msg}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "has_drift": self.has_drift,
            "missing_columns": self.missing_columns,
            "extra_columns": self.extra_columns,
            "type_mismatches": self.type_mismatches,
        }


def watch(df: pd.DataFrame, schema: PipelineSchema) -> DriftReport:
    """Compare *df* against *schema* and return a :class:`DriftReport`."""
    report = DriftReport()

    schema_cols = {col.name: col for col in schema.columns}
    df_cols = set(df.columns)

    report.missing_columns = [name for name in schema_cols if name not in df_cols]
    report.extra_columns = [name for name in df_cols if name not in schema_cols]

    for name, col_schema in schema_cols.items():
        if name not in df_cols:
            continue
        inferred = _infer_dtype(df[name])
        if inferred != col_schema.dtype:
            report.type_mismatches.append(
                f"'{name}': expected '{col_schema.dtype}', got '{inferred}'"
            )

    return report
