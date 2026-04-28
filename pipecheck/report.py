"""Combines validation results and profiling data into a structured report."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from pipecheck.profiler import DataFrameProfile
from pipecheck.validator import ValidationResult


@dataclass
class PipeCheckReport:
    schema_name: str
    passed: bool
    error_count: int
    errors: List[Dict[str, str]]
    profile: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_name": self.schema_name,
            "passed": self.passed,
            "error_count": self.error_count,
            "errors": self.errors,
            "profile": self.profile,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def print_summary(self) -> None:
        status = "PASSED" if self.passed else "FAILED"
        print(f"\n=== PipeCheck Report: {self.schema_name} ===")
        print(f"Status      : {status}")
        print(f"Errors      : {self.error_count}")
        print(f"Rows        : {self.profile.get('row_count', 'N/A')}")
        print(f"Columns     : {self.profile.get('column_count', 'N/A')}")
        if self.errors:
            print("\nValidation Errors:")
            for err in self.errors:
                print(f"  [{err['column']}] {err['rule']}: {err['message']}")
        print()


def build_report(
    schema_name: str,
    validation_result: ValidationResult,
    df_profile: DataFrameProfile,
) -> PipeCheckReport:
    """Assemble a :class:`PipeCheckReport` from validation and profiling outputs."""
    errors = [
        {
            "column": e.column,
            "rule": e.rule,
            "message": e.message,
        }
        for e in validation_result.errors
    ]

    return PipeCheckReport(
        schema_name=schema_name,
        passed=validation_result.passed,
        error_count=len(errors),
        errors=errors,
        profile=df_profile.as_dict(),
    )
