"""Tests for pipecheck.snapshot_diff."""

import pytest

from pipecheck.snapshot_diff import ColumnStatDiff, SnapshotDiff, diff_snapshots
from pipecheck.snapshotter import Snapshot


def _make_snapshot(run_id: str, rows: int = 10, cols=None, stats=None) -> Snapshot:
    cols = cols or ["a", "b"]
    stats = stats or {c: {"null_count": 0, "mean": 1.0, "min": 0.0, "max": 5.0, "unique_count": 3} for c in cols}
    return Snapshot(
        run_id=run_id,
        timestamp="2024-01-01T00:00:00+00:00",
        row_count=rows,
        column_count=len(cols),
        columns=cols,
        column_stats=stats,
    )


class TestSnapshotDiffProperties:
    def test_no_changes_has_changes_false(self):
        s = _make_snapshot("r1")
        diff = diff_snapshots(s, s)
        assert not diff.has_changes

    def test_row_count_change_triggers_has_changes(self):
        before = _make_snapshot("r1", rows=10)
        after = _make_snapshot("r2", rows=20)
        diff = diff_snapshots(before, after)
        assert diff.has_changes

    def test_added_column_detected(self):
        before = _make_snapshot("r1", cols=["a"])
        after = _make_snapshot("r2", cols=["a", "b"])
        diff = diff_snapshots(before, after)
        assert "b" in diff.added_columns

    def test_removed_column_detected(self):
        before = _make_snapshot("r1", cols=["a", "b"])
        after = _make_snapshot("r2", cols=["a"])
        diff = diff_snapshots(before, after)
        assert "b" in diff.removed_columns

    def test_stat_change_detected(self):
        before = _make_snapshot("r1", stats={"a": {"null_count": 0, "mean": 1.0, "min": 0.0, "max": 5.0, "unique_count": 3}})
        after = _make_snapshot("r2", stats={"a": {"null_count": 2, "mean": 1.0, "min": 0.0, "max": 5.0, "unique_count": 3}})
        diff = diff_snapshots(before, after)
        col_diff = next(cd for cd in diff.column_diffs if cd.column == "a")
        assert "null_count" in col_diff.changes
        assert col_diff.changes["null_count"] == {"before": 0, "after": 2}


class TestSnapshotDiffSummary:
    def test_summary_no_changes(self):
        s = _make_snapshot("r1")
        diff = diff_snapshots(s, s)
        assert "No changes detected" in diff.summary()

    def test_summary_includes_run_ids(self):
        before = _make_snapshot("run_before")
        after = _make_snapshot("run_after")
        diff = diff_snapshots(before, after)
        summary = diff.summary()
        assert "run_before" in summary
        assert "run_after" in summary

    def test_summary_mentions_added_column(self):
        before = _make_snapshot("r1", cols=["a"])
        after = _make_snapshot("r2", cols=["a", "new_col"])
        diff = diff_snapshots(before, after)
        assert "new_col" in diff.summary()


class TestSnapshotDiffToDict:
    def test_to_dict_keys(self):
        s = _make_snapshot("r1")
        d = diff_snapshots(s, s).to_dict()
        assert set(d.keys()) == {
            "run_id_before", "run_id_after",
            "row_count_before", "row_count_after",
            "added_columns", "removed_columns", "column_diffs",
        }

    def test_column_diffs_serializable(self):
        s = _make_snapshot("r1")
        d = diff_snapshots(s, s).to_dict()
        assert isinstance(d["column_diffs"], list)
