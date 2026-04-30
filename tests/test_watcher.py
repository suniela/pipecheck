"""Tests for pipecheck.watcher."""

import pandas as pd
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.watcher import DriftReport, watch


def _make_schema(*cols: tuple) -> PipelineSchema:
    """Helper: build a PipelineSchema from (name, dtype) tuples."""
    columns = [ColumnSchema(name=n, dtype=d) for n, d in cols]
    return PipelineSchema(name="test", columns=columns)


@pytest.fixture
def base_schema():
    return _make_schema(("id", "int"), ("name", "str"), ("amount", "float"))


@pytest.fixture
def matching_df():
    return pd.DataFrame({"id": [1, 2], "name": ["a", "b"], "amount": [1.0, 2.0]})


class TestDriftReportProperties:
    def test_no_drift_has_drift_false(self):
        report = DriftReport()
        assert report.has_drift is False

    def test_missing_column_triggers_drift(self):
        report = DriftReport(missing_columns=["id"])
        assert report.has_drift is True

    def test_extra_column_triggers_drift(self):
        report = DriftReport(extra_columns=["unknown"])
        assert report.has_drift is True

    def test_type_mismatch_triggers_drift(self):
        report = DriftReport(type_mismatches=["'id': expected 'int', got 'str'"])
        assert report.has_drift is True

    def test_summary_no_drift(self):
        assert DriftReport().summary() == "No schema drift detected."

    def test_summary_lists_issues(self):
        report = DriftReport(missing_columns=["id"], extra_columns=["foo"])
        summary = report.summary()
        assert "MISSING" in summary
        assert "EXTRA" in summary
        assert "id" in summary
        assert "foo" in summary

    def test_to_dict_keys(self):
        report = DriftReport()
        d = report.to_dict()
        assert set(d.keys()) == {"has_drift", "missing_columns", "extra_columns", "type_mismatches"}


class TestWatch:
    def test_no_drift_on_matching_df(self, base_schema, matching_df):
        report = watch(matching_df, base_schema)
        assert report.has_drift is False

    def test_detects_missing_column(self, base_schema):
        df = pd.DataFrame({"id": [1], "name": ["a"]})
        report = watch(df, base_schema)
        assert "amount" in report.missing_columns

    def test_detects_extra_column(self, base_schema, matching_df):
        df = matching_df.copy()
        df["extra"] = "x"
        report = watch(df, base_schema)
        assert "extra" in report.extra_columns

    def test_detects_type_mismatch(self, base_schema):
        df = pd.DataFrame({"id": ["one", "two"], "name": ["a", "b"], "amount": [1.0, 2.0]})
        report = watch(df, base_schema)
        assert any("id" in m for m in report.type_mismatches)

    def test_no_false_positive_on_bool(self):
        schema = _make_schema(("active", "bool"))
        df = pd.DataFrame({"active": [True, False]})
        report = watch(df, schema)
        assert not report.type_mismatches

    def test_no_false_positive_on_datetime(self):
        schema = _make_schema(("ts", "datetime"))
        df = pd.DataFrame({"ts": pd.to_datetime(["2024-01-01", "2024-01-02"])})
        report = watch(df, schema)
        assert not report.type_mismatches
