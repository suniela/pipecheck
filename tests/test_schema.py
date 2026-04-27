"""Tests for pipecheck.schema — schema loading and validation."""

import json
import pytest
from pathlib import Path

from pipecheck.schema import (
    ColumnSchema,
    PipelineSchema,
    load_schema,
    VALID_TYPES,
)


SAMPLE_SCHEMA = {
    "name": "orders",
    "version": "2.1",
    "columns": [
        {"name": "order_id", "type": "integer", "nullable": False, "unique": True},
        {"name": "status", "type": "string", "allowed_values": ["pending", "shipped", "delivered"]},
        {"name": "amount", "type": "float", "min_value": 0.0, "max_value": 99999.99},
    ],
}


@pytest.fixture()
def schema_file(tmp_path: Path) -> Path:
    p = tmp_path / "orders_schema.json"
    p.write_text(json.dumps(SAMPLE_SCHEMA), encoding="utf-8")
    return p


class TestColumnSchema:
    def test_valid_column(self):
        col = ColumnSchema(name="price", dtype="float", nullable=False)
        assert col.name == "price"
        assert col.nullable is False

    def test_invalid_dtype_raises(self):
        with pytest.raises(ValueError, match="unsupported type"):
            ColumnSchema(name="bad", dtype="unknown")

    def test_all_valid_types_accepted(self):
        for vtype in VALID_TYPES:
            col = ColumnSchema(name="x", dtype=vtype)
            assert col.dtype == vtype


class TestLoadSchema:
    def test_loads_name_and_version(self, schema_file: Path):
        schema = load_schema(schema_file)
        assert schema.name == "orders"
        assert schema.version == "2.1"

    def test_loads_columns(self, schema_file: Path):
        schema = load_schema(schema_file)
        assert len(schema.columns) == 3
        assert schema.column_names == ["order_id", "status", "amount"]

    def test_column_constraints_preserved(self, schema_file: Path):
        schema = load_schema(schema_file)
        order_id = schema.columns[0]
        assert order_id.nullable is False
        assert order_id.unique is True

        amount = schema.columns[2]
        assert amount.min_value == 0.0
        assert amount.max_value == 99999.99

    def test_missing_file_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            load_schema(tmp_path / "nonexistent.json")

    def test_defaults_when_keys_absent(self, tmp_path: Path):
        minimal = {"columns": [{"name": "col1", "type": "string"}]}
        p = tmp_path / "minimal.json"
        p.write_text(json.dumps(minimal))
        schema = load_schema(p)
        assert schema.version == "1.0"
        assert schema.name == "minimal"
