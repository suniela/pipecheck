"""Schema contract definition and loading utilities for pipecheck."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


VALID_TYPES = {"string", "integer", "float", "boolean", "date", "datetime", "any"}


@dataclass
class ColumnSchema:
    name: str
    dtype: str
    nullable: bool = True
    unique: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[List[Any]] = None

    def __post_init__(self) -> None:
        if self.dtype not in VALID_TYPES:
            raise ValueError(
                f"Column '{self.name}': unsupported type '{self.dtype}'. "
                f"Must be one of {sorted(VALID_TYPES)}."
            )


@dataclass
class PipelineSchema:
    name: str
    version: str
    columns: List[ColumnSchema] = field(default_factory=list)

    @property
    def column_names(self) -> List[str]:
        return [c.name for c in self.columns]


def load_schema(path: str | Path) -> PipelineSchema:
    """Load and parse a JSON schema contract file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")

    with path.open("r", encoding="utf-8") as fh:
        raw: Dict[str, Any] = json.load(fh)

    name = raw.get("name", path.stem)
    version = raw.get("version", "1.0")
    columns = [
        ColumnSchema(
            name=col["name"],
            dtype=col["type"],
            nullable=col.get("nullable", True),
            unique=col.get("unique", False),
            min_value=col.get("min_value"),
            max_value=col.get("max_value"),
            allowed_values=col.get("allowed_values"),
        )
        for col in raw.get("columns", [])
    ]
    return PipelineSchema(name=name, version=version, columns=columns)
