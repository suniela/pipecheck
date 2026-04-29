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

    def get_column(self, name: str) -> Optional[ColumnProfile]:
        """Return the :class:`ColumnProfile` for *name*, or ``None`` if not found."""
        for col in self.columns:
            if col.name == name:
                return col
        return None


def _profile_numeric(series: pd.Series) -> tuple[Any, Any, Optional[float]]:
    """Return (min, max, mean) for a numeric series, ignoring nulls."""
    non_null = series.dropna()
    if non_null.empty:
        return None, None, None
    return non_null.min(), non_null.max(), float(non_null.mean())


def _profile_datetime(series: pd.Series) -> tuple[Any, Any]:
    """Return (min, max) as ISO strings for a datetime series, ignoring nulls."""
    non_null = series.dropna()
    if non_null.empty:
        return None, None
    return str(non_null.min()), str(non_null.max())


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
            min_val, max_val, mean_val = _profile_numeric(series)
        elif pd.api.types.is_datetime64_any_dtype(series):
            min_val, max_val = _profile_datetime(series)

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
