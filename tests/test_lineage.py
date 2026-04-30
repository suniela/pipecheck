"""Tests for pipecheck.lineage module."""
import json
import pytest
from pipecheck.lineage import (
    LineageEntry,
    LineageLog,
    build_lineage_entry,
)


def _make_entry(
    run_id="run-001",
    source="orders.csv",
    schema_name="orders",
    row_count=100,
    column_count=5,
    passed=True,
    quality_score=0.95,
    drift_detected=False,
    tags=None,
) -> LineageEntry:
    return build_lineage_entry(
        run_id=run_id,
        source=source,
        schema_name=schema_name,
        row_count=row_count,
        column_count=column_count,
        passed=passed,
        quality_score=quality_score,
        drift_detected=drift_detected,
        tags=tags,
    )


class TestLineageEntry:
    def test_to_dict_contains_expected_keys(self):
        entry = _make_entry()
        d = entry.to_dict()
        expected = {
            "run_id", "source", "schema_name", "row_count",
            "column_count", "passed", "quality_score",
            "drift_detected", "timestamp", "tags",
        }
        assert expected == set(d.keys())

    def test_timestamp_is_set_automatically(self):
        entry = _make_entry()
        assert entry.timestamp is not None
        assert "T" in entry.timestamp  # ISO format

    def test_tags_default_empty(self):
        entry = _make_entry()
        assert entry.tags == []

    def test_tags_preserved(self):
        entry = _make_entry(tags=["prod", "nightly"])
        assert entry.tags == ["prod", "nightly"]

    def test_drift_detected_false_by_default(self):
        entry = _make_entry()
        assert entry.drift_detected is False


class TestLineageLog:
    def test_empty_log_latest_returns_none(self):
        log = LineageLog()
        assert log.latest() is None

    def test_add_and_latest(self):
        log = LineageLog()
        e = _make_entry(run_id="r1")
        log.add(e)
        assert log.latest().run_id == "r1"

    def test_latest_returns_last_added(self):
        log = LineageLog()
        log.add(_make_entry(run_id="r1"))
        log.add(_make_entry(run_id="r2"))
        assert log.latest().run_id == "r2"

    def test_for_schema_filters_correctly(self):
        log = LineageLog()
        log.add(_make_entry(schema_name="orders"))
        log.add(_make_entry(schema_name="users"))
        log.add(_make_entry(schema_name="orders"))
        results = log.for_schema("orders")
        assert len(results) == 2
        assert all(e.schema_name == "orders" for e in results)

    def test_for_schema_no_match_returns_empty(self):
        log = LineageLog()
        log.add(_make_entry(schema_name="orders"))
        assert log.for_schema("unknown") == []

    def test_to_json_is_valid_json(self):
        log = LineageLog()
        log.add(_make_entry())
        parsed = json.loads(log.to_json())
        assert "entries" in parsed
        assert len(parsed["entries"]) == 1

    def test_to_dict_entries_list(self):
        log = LineageLog()
        log.add(_make_entry(run_id="x"))
        d = log.to_dict()
        assert d["entries"][0]["run_id"] == "x"
