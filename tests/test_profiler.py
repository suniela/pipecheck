"""Tests for pipecheck.profiler."""

from __future__ import annotations

import pandas as pd
import pytest

from pipecheck.profiler import ColumnProfile, DataFrameProfile, profile


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "price": [10.5, 20.0, None, 40.0, 50.0],
            "label": ["a", "b", "a", None, "c"],
        }
    )


class TestProfile:
    def test_returns_dataframe_profile(self, sample_df):
        result = profile(sample_df)
        assert isinstance(result, DataFrameProfile)

    def test_row_and_column_counts(self, sample_df):
        result = profile(sample_df)
        assert result.row_count == 5
        assert result.column_count == 3

    def test_column_names_present(self, sample_df):
        result = profile(sample_df)
        names = [c.name for c in result.columns]
        assert names == ["id", "price", "label"]

    def test_null_count_for_price(self, sample_df):
        result = profile(sample_df)
        price_profile = next(c for c in result.columns if c.name == "price")
        assert price_profile.null_count == 1
        assert abs(price_profile.null_pct - 0.2) < 1e-6

    def test_numeric_stats_computed(self, sample_df):
        result = profile(sample_df)
        id_profile = next(c for c in result.columns if c.name == "id")
        assert id_profile.min == 1
        assert id_profile.max == 5
        assert id_profile.mean == pytest.approx(3.0)

    def test_non_numeric_has_no_mean(self, sample_df):
        result = profile(sample_df)
        label_profile = next(c for c in result.columns if c.name == "label")
        assert label_profile.mean is None

    def test_unique_count(self, sample_df):
        result = profile(sample_df)
        label_profile = next(c for c in result.columns if c.name == "label")
        # values: a, b, a, None, c  -> unique including NaN
        assert label_profile.unique_count == 4

    def test_as_dict_structure(self, sample_df):
        result = profile(sample_df)
        d = result.as_dict()
        assert "row_count" in d
        assert "column_count" in d
        assert isinstance(d["columns"], list)
        assert "null_pct" in d["columns"][0]

    def test_empty_dataframe(self):
        df = pd.DataFrame({"x": pd.Series([], dtype="float64")})
        result = profile(df)
        assert result.row_count == 0
        assert result.columns[0].null_pct == 0.0
