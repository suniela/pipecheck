"""Lineage tracking for ETL pipeline runs."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional
import json


@dataclass
class LineageEntry:
    """Records a single pipeline run's lineage metadata."""
    run_id: str
    source: str
    schema_name: str
    row_count: int
    column_count: int
    passed: bool
    quality_score: Optional[float] = None
    drift_detected: bool = False
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "source": self.source,
            "schema_name": self.schema_name,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "passed": self.passed,
            "quality_score": self.quality_score,
            "drift_detected": self.drift_detected,
            "timestamp": self.timestamp,
            "tags": self.tags,
        }


@dataclass
class LineageLog:
    """Ordered collection of lineage entries."""
    entries: List[LineageEntry] = field(default_factory=list)

    def add(self, entry: LineageEntry) -> None:
        self.entries.append(entry)

    def latest(self) -> Optional[LineageEntry]:
        return self.entries[-1] if self.entries else None

    def for_schema(self, schema_name: str) -> List[LineageEntry]:
        return [e for e in self.entries if e.schema_name == schema_name]

    def to_dict(self) -> dict:
        return {"entries": [e.to_dict() for e in self.entries]}

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


def build_lineage_entry(
    run_id: str,
    source: str,
    schema_name: str,
    row_count: int,
    column_count: int,
    passed: bool,
    quality_score: Optional[float] = None,
    drift_detected: bool = False,
    tags: Optional[List[str]] = None,
) -> LineageEntry:
    """Convenience factory for creating a LineageEntry."""
    return LineageEntry(
        run_id=run_id,
        source=source,
        schema_name=schema_name,
        row_count=row_count,
        column_count=column_count,
        passed=passed,
        quality_score=quality_score,
        drift_detected=drift_detected,
        tags=tags or [],
    )
