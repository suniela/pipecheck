"""Tests for pipecheck.snapshotter."""

import json
from pathlib import Path

import pandas as pd
import pytest

from pipecheck.snapshotter import (
    Snapshot,
    list_snapshots,
    load_snapshot,
    save_snapshot,
    take_snapshot,
)


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "amount": [10.5, 20.0, 15.75],
            "status": ["paid", "pending", "paid"],
        }
    )


class TestTakeSnapshot:
    def test_returns_snapshot_instance(self, sample_df):
        snap = take_snapshot(sample_df, run_id="run_001")
        assert isinstance(snap, Snapshot)

    def test_run_id_preserved(self, sample_df):
        snap = take_snapshot(sample_df, run_id="run_abc")
        assert snap.run_id == "run_abc"

    def test_row_and_column_counts(self, sample_df):
        snap = take_snapshot(sample_df, run_id="r1")
        assert snap.row_count == 3
        assert snap.column_count == 3

    def test_columns_listed(self, sample_df):
        snap = take_snapshot(sample_df, run_id="r1")
        assert set(snap.columns) == {"order_id", "amount", "status"}

    def test_tags_stored(self, sample_df):
        snap = take_snapshot(sample_df, run_id="r1", tags={"env": "prod"})
        assert snap.tags == {"env": "prod"}

    def test_timestamp_is_set(self, sample_df):
        snap = take_snapshot(sample_df, run_id="r1")
        assert snap.timestamp  # non-empty string


class TestSaveLoadSnapshot:
    def test_save_creates_file(self, sample_df, tmp_path):
        snap = take_snapshot(sample_df, run_id="run_save")
        path = save_snapshot(snap, directory=tmp_path)
        assert path.exists()

    def test_saved_file_is_valid_json(self, sample_df, tmp_path):
        snap = take_snapshot(sample_df, run_id="run_json")
        path = save_snapshot(snap, directory=tmp_path)
        data = json.loads(path.read_text())
        assert data["run_id"] == "run_json"

    def test_load_roundtrip(self, sample_df, tmp_path):
        snap = take_snapshot(sample_df, run_id="run_rt")
        save_snapshot(snap, directory=tmp_path)
        loaded = load_snapshot("run_rt", directory=tmp_path)
        assert loaded.run_id == snap.run_id
        assert loaded.row_count == snap.row_count

    def test_load_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_snapshot("nonexistent", directory=tmp_path)


class TestListSnapshots:
    def test_empty_directory_returns_empty(self, tmp_path):
        assert list_snapshots(tmp_path) == []

    def test_nonexistent_directory_returns_empty(self, tmp_path):
        assert list_snapshots(tmp_path / "missing") == []

    def test_lists_saved_run_ids(self, sample_df, tmp_path):
        for rid in ["run_b", "run_a", "run_c"]:
            save_snapshot(take_snapshot(sample_df, run_id=rid), directory=tmp_path)
        result = list_snapshots(tmp_path)
        assert result == ["run_a", "run_b", "run_c"]
