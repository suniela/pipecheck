"""Schema diff utility: compare two PipelineSchema objects and report differences."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.schema import PipelineSchema


@dataclass
class SchemaDiff:
    """Holds the result of comparing two pipeline schemas."""

    added: List[str] = field(default_factory=list)      # columns in new, not in old
    removed: List[str] = field(default_factory=list)    # columns in old, not in new
    changed: List[dict] = field(default_factory=list)   # columns present in both but different

    @property
    def has_changes(self) -> bool:
        return bool(self.added or self.removed or self.changed)

    def summary(self) -> str:
        lines = []
        if not self.has_changes:
            return "Schemas are identical."
        if self.added:
            lines.append(f"Added columns   ({len(self.added)}): {', '.join(self.added)}")
        if self.removed:
            lines.append(f"Removed columns ({len(self.removed)}): {', '.join(self.removed)}")
        for change in self.changed:
            col = change["column"]
            diffs = ", ".join(
                f"{k}: {change['old'][k]!r} -> {change['new'][k]!r}"
                for k in change["old"]
            )
            lines.append(f"Changed column  '{col}': {diffs}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "added": self.added,
            "removed": self.removed,
            "changed": self.changed,
        }


def diff_schemas(old: PipelineSchema, new: PipelineSchema) -> SchemaDiff:
    """Compare *old* schema against *new* schema and return a SchemaDiff."""
    old_map = {c.name: c for c in old.columns}
    new_map = {c.name: c for c in new.columns}

    old_names = set(old_map)
    new_names = set(new_map)

    added = sorted(new_names - old_names)
    removed = sorted(old_names - new_names)

    changed = []
    for name in sorted(old_names & new_names):
        o, n = old_map[name], new_map[name]
        diffs_old: dict = {}
        diffs_new: dict = {}
        for attr in ("dtype", "nullable", "unique"):
            ov, nv = getattr(o, attr), getattr(n, attr)
            if ov != nv:
                diffs_old[attr] = ov
                diffs_new[attr] = nv
        if diffs_old:
            changed.append({"column": name, "old": diffs_old, "new": diffs_new})

    return SchemaDiff(added=added, removed=removed, changed=changed)
