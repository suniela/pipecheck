"""Lightweight profiler that computes per-column statistics for a DataFrame."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    row_count: int
    null_count: int
    null_pct: float
    unique_count: int
    min: Optional[Any] = None
    max: Optional[Any] = None
    mean: Optional[float] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "dtype": self.dtype,
            "row_count": self.row_count,
            "null_count": self.null_count,
            "null_pct": round(self.null_pct, 4),
            "unique_count": self.unique_count,
            "min": self.min,
            "max": self.max,
            "mean": round(self.mean, 4) if self.mean is not None else None,
        }


@dataclass
class DataFrameProfile:
    row_count: int
    column_count: int
    columns: List[ColumnProfile] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": [c.as_dict() for c in self.columns],
        }


def profile(df: pd.DataFrame) -> DataFrameProfile:
    """Compute a :class:`DataFrameProfile` for the given DataFrame."""
    col_profiles: list[ColumnProfile] = []

    for col_name in df.columns:
        series = df[col_name]
        row_count = len(series)
        null_count = int(series.isna().sum())
        null_pct = null_count / row_count if row_count > 0 else 0.0
        unique_count = int(series.nunique(dropna=False))

        min_val: Any = None
        max_val: Any = None
        mean_val: Optional[float] = None

        if pd.api.types.is_numeric_dtype(series):
            non_null = series.dropna()
            if not non_null.empty:
                min_val = non_null.min()
                max_val = non_null.max()
                mean_val = float(non_null.mean())
        elif pd.api.types.is_datetime64_any_dtype(series):
            non_null = series.dropna()
            if not non_null.empty:
                min_val = str(non_null.min())
                max_val = str(non_null.max())

        col_profiles.append(
            ColumnProfile(
                name=col_name,
                dtype=str(series.dtype),
                row_count=row_count,
                null_count=null_count,
                null_pct=null_pct,
                unique_count=unique_count,
                min=min_val,
                max=max_val,
                mean=mean_val,
            )
        )

    return DataFrameProfile(
        row_count=len(df),
        column_count=len(df.columns),
        columns=col_profiles,
    )
