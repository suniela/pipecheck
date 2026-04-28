"""Tests for pipecheck.validator."""

from __future__ import annotations

import pandas as pd
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.validator import ValidationError, ValidationResult, validate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_schema(**overrides) -> PipelineSchema:
    """Return a simple schema with one column, optionally overriding fields."""
    col_kwargs = {"name": "id", "dtype": "int", "nullable": False, "unique": True}
    col_kwargs.update(overrides)
    return PipelineSchema(name="test", columns=[ColumnSchema(**col_kwargs)])


# ---------------------------------------------------------------------------
# ValidationResult
# ---------------------------------------------------------------------------

class TestValidationResult:
    def test_passed_summary(self):
        result = ValidationResult(passed=True)
        assert "passed" in result.summary()

    def test_failed_summary_lists_errors(self):
        err = ValidationError(column="id", rule="null_violation", message="has nulls")
        result = ValidationResult(passed=False, errors=[err])
        summary = result.summary()
        assert "1 error" in summary
        assert "id" in summary


# ---------------------------------------------------------------------------
# validate()
# ---------------------------------------------------------------------------

class TestValidate:
    def test_valid_dataframe_passes(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        schema = _make_schema()
        result = validate(df, schema)
        assert result.passed
        assert result.errors == []

    def test_missing_column_raises_error(self):
        df = pd.DataFrame({"other": [1, 2, 3]})
        schema = _make_schema()
        result = validate(df, schema)
        assert not result.passed
        assert any(e.rule == "missing_column" for e in result.errors)

    def test_dtype_mismatch_detected(self):
        df = pd.DataFrame({"id": ["a", "b", "c"]})
        schema = _make_schema(dtype="int")
        result = validate(df, schema)
        assert not result.passed
        assert any(e.rule == "dtype_mismatch" for e in result.errors)

    def test_null_violation_detected(self):
        df = pd.DataFrame({"id": pd.array([1, None, 3], dtype="Int64")})
        schema = _make_schema(dtype="int", nullable=False)
        result = validate(df, schema)
        assert not result.passed
        assert any(e.rule == "null_violation" for e in result.errors)

    def test_nullable_column_allows_nulls(self):
        df = pd.DataFrame({"id": pd.array([1, None, 3], dtype="Int64")})
        schema = _make_schema(dtype="int", nullable=True, unique=False)
        result = validate(df, schema)
        # Only check that null_violation is not raised
        assert not any(e.rule == "null_violation" for e in result.errors)

    def test_unique_violation_detected(self):
        df = pd.DataFrame({"id": [1, 1, 2]})
        schema = _make_schema(unique=True)
        result = validate(df, schema)
        assert not result.passed
        assert any(e.rule == "unique_violation" for e in result.errors)

    def test_multiple_errors_collected(self):
        df = pd.DataFrame({"id": ["x", "x", None]})
        schema = _make_schema(dtype="int", nullable=False, unique=True)
        result = validate(df, schema)
        rules = {e.rule for e in result.errors}
        assert "dtype_mismatch" in rules
