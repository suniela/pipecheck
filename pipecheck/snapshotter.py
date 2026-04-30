"""Snapshot management for tracking pipeline output history."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from pipecheck.profiler import profile_dataframe


_DEFAULT_SNAPSHOT_DIR = Path(".pipecheck_snapshots")


@dataclass
class Snapshot:
    """Represents a single recorded snapshot of a DataFrame profile."""

    run_id: str
    timestamp: str
    row_count: int
    column_count: int
    columns: List[str]
    column_stats: Dict[str, Dict[str, Any]]
    tags: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": self.columns,
            "column_stats": self.column_stats,
            "tags": self.tags,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Snapshot":
        return cls(
            run_id=data["run_id"],
            timestamp=data["timestamp"],
            row_count=data["row_count"],
            column_count=data["column_count"],
            columns=data["columns"],
            column_stats=data["column_stats"],
            tags=data.get("tags", {}),
        )


def take_snapshot(
    df: pd.DataFrame,
    run_id: str,
    tags: Optional[Dict[str, str]] = None,
) -> Snapshot:
    """Create a Snapshot from a DataFrame using its profile."""
    dfp = profile_dataframe(df)
    timestamp = datetime.now(timezone.utc).isoformat()
    column_stats = {
        col: dfp.get_column(col).as_dict() for col in dfp.columns
    }
    return Snapshot(
        run_id=run_id,
        timestamp=timestamp,
        row_count=dfp.row_count,
        column_count=dfp.column_count,
        columns=list(dfp.columns),
        column_stats=column_stats,
        tags=tags or {},
    )


def save_snapshot(
    snapshot: Snapshot,
    directory: Path = _DEFAULT_SNAPSHOT_DIR,
) -> Path:
    """Persist a snapshot as a JSON file. Returns the written path."""
    directory.mkdir(parents=True, exist_ok=True)
    filename = f"{snapshot.run_id}.json"
    path = directory / filename
    path.write_text(json.dumps(snapshot.to_dict(), indent=2))
    return path


def load_snapshot(run_id: str, directory: Path = _DEFAULT_SNAPSHOT_DIR) -> Snapshot:
    """Load a previously saved snapshot by run_id."""
    path = directory / f"{run_id}.json"
    if not path.exists():
        raise FileNotFoundError(f"No snapshot found for run_id '{run_id}' in {directory}")
    data = json.loads(path.read_text())
    return Snapshot.from_dict(data)


def list_snapshots(directory: Path = _DEFAULT_SNAPSHOT_DIR) -> List[str]:
    """Return sorted list of available run_ids in the snapshot directory."""
    if not directory.exists():
        return []
    return sorted(
        p.stem for p in directory.glob("*.json")
    )
