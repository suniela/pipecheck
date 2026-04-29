"""Tests for pipecheck.differ — schema diff utility."""

import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.differ import SchemaDiff, diff_schemas


def _make_schema(*cols: ColumnSchema) -> PipelineSchema:
    return PipelineSchema(name="test", columns=list(cols))


COL_ID = ColumnSchema(name="id", dtype="int64", nullable=False)
COL_NAME = ColumnSchema(name="name", dtype="string", nullable=True)
COL_PRICE = ColumnSchema(name="price", dtype="float64", nullable=True)


class TestSchemaDiffProperties:
    def test_no_changes_has_changes_false(self):
        diff = SchemaDiff()
        assert not diff.has_changes

    def test_added_triggers_has_changes(self):
        diff = SchemaDiff(added=["new_col"])
        assert diff.has_changes

    def test_removed_triggers_has_changes(self):
        diff = SchemaDiff(removed=["old_col"])
        assert diff.has_changes

    def test_changed_triggers_has_changes(self):
        diff = SchemaDiff(changed=[{"column": "x", "old": {"dtype": "int64"}, "new": {"dtype": "string"}}])
        assert diff.has_changes

    def test_summary_identical(self):
        assert SchemaDiff().summary() == "Schemas are identical."

    def test_summary_lists_added(self):
        diff = SchemaDiff(added=["foo", "bar"])
        assert "foo" in diff.summary()
        assert "Added" in diff.summary()

    def test_to_dict_keys(self):
        diff = SchemaDiff(added=["a"])
        d = diff.to_dict()
        assert set(d.keys()) == {"added", "removed", "changed"}


class TestDiffSchemas:
    def test_identical_schemas_no_diff(self):
        schema = _make_schema(COL_ID, COL_NAME)
        result = diff_schemas(schema, schema)
        assert not result.has_changes

    def test_detects_added_column(self):
        old = _make_schema(COL_ID)
        new = _make_schema(COL_ID, COL_NAME)
        result = diff_schemas(old, new)
        assert "name" in result.added
        assert result.removed == []

    def test_detects_removed_column(self):
        old = _make_schema(COL_ID, COL_NAME)
        new = _make_schema(COL_ID)
        result = diff_schemas(old, new)
        assert "name" in result.removed
        assert result.added == []

    def test_detects_dtype_change(self):
        old = _make_schema(COL_ID, COL_NAME)
        new_name = ColumnSchema(name="name", dtype="object", nullable=True)
        new = _make_schema(COL_ID, new_name)
        result = diff_schemas(old, new)
        assert len(result.changed) == 1
        assert result.changed[0]["column"] == "name"
        assert result.changed[0]["old"]["dtype"] == "string"
        assert result.changed[0]["new"]["dtype"] == "object"

    def test_detects_nullable_change(self):
        old = _make_schema(COL_ID)
        new_id = ColumnSchema(name="id", dtype="int64", nullable=True)
        new = _make_schema(new_id)
        result = diff_schemas(old, new)
        assert result.changed[0]["old"]["nullable"] is False
        assert result.changed[0]["new"]["nullable"] is True

    def test_summary_contains_change_details(self):
        old = _make_schema(COL_ID)
        new_id = ColumnSchema(name="id", dtype="string", nullable=False)
        new = _make_schema(new_id)
        result = diff_schemas(old, new)
        summary = result.summary()
        assert "id" in summary
        assert "int64" in summary
        assert "string" in summary
