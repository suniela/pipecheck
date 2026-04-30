"""Compare two snapshots to detect statistical drift between pipeline runs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pipecheck.snapshotter import Snapshot


@dataclass
class ColumnStatDiff:
    """Difference in statistics for a single column between two snapshots."""

    column: str
    changes: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @property
    def has_changes(self) -> bool:
        return bool(self.changes)


@dataclass
class SnapshotDiff:
    """Full diff between two Snapshots."""

    run_id_before: str
    run_id_after: str
    row_count_before: int
    row_count_after: int
    added_columns: List[str] = field(default_factory=list)
    removed_columns: List[str] = field(default_factory=list)
    column_diffs: List[ColumnStatDiff] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(
            self.added_columns
            or self.removed_columns
            or any(cd.has_changes for cd in self.column_diffs)
            or self.row_count_before != self.row_count_after
        )

    def summary(self) -> str:
        lines = [
            f"Snapshot diff: {self.run_id_before} → {self.run_id_after}",
            f"  Rows: {self.row_count_before} → {self.row_count_after}",
        ]
        if self.added_columns:
            lines.append(f"  Added columns: {', '.join(self.added_columns)}")
        if self.removed_columns:
            lines.append(f"  Removed columns: {', '.join(self.removed_columns)}")
        for cd in self.column_diffs:
            if cd.has_changes:
                for stat, vals in cd.changes.items():
                    lines.append(
                        f"  [{cd.column}] {stat}: {vals['before']} → {vals['after']}"
                    )
        if not self.has_changes:
            lines.append("  No changes detected.")
        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id_before": self.run_id_before,
            "run_id_after": self.run_id_after,
            "row_count_before": self.row_count_before,
            "row_count_after": self.row_count_after,
            "added_columns": self.added_columns,
            "removed_columns": self.removed_columns,
            "column_diffs": [
                {"column": cd.column, "changes": cd.changes}
                for cd in self.column_diffs
            ],
        }


_TRACKED_STATS = ("null_count", "mean", "min", "max", "unique_count")


def diff_snapshots(before: Snapshot, after: Snapshot) -> SnapshotDiff:
    """Compute the diff between two snapshots."""
    before_cols = set(before.columns)
    after_cols = set(after.columns)

    added = sorted(after_cols - before_cols)
    removed = sorted(before_cols - after_cols)
    common = sorted(before_cols & after_cols)

    column_diffs: List[ColumnStatDiff] = []
    for col in common:
        b_stats = before.column_stats.get(col, {})
        a_stats = after.column_stats.get(col, {})
        changes: Dict[str, Dict[str, Any]] = {}
        for stat in _TRACKED_STATS:
            bv = b_stats.get(stat)
            av = a_stats.get(stat)
            if bv != av:
                changes[stat] = {"before": bv, "after": av}
        column_diffs.append(ColumnStatDiff(column=col, changes=changes))

    return SnapshotDiff(
        run_id_before=before.run_id,
        run_id_after=after.run_id,
        row_count_before=before.row_count,
        row_count_after=after.row_count,
        added_columns=added,
        removed_columns=removed,
        column_diffs=column_diffs,
    )
