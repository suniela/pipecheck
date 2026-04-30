"""Pipeline quality scorer — aggregates validation and drift results into a numeric score."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipecheck.validator import ValidationResult
from pipecheck.watcher import DriftReport


_VALIDATION_WEIGHT = 0.6
_DRIFT_WEIGHT = 0.4


@dataclass
class QualityScore:
    """Holds the computed quality score and its components."""

    validation_score: float  # 0.0 – 1.0
    drift_score: float       # 0.0 – 1.0
    overall_score: float     # weighted combination
    grade: str               # A / B / C / D / F
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "validation_score": round(self.validation_score, 4),
            "drift_score": round(self.drift_score, 4),
            "overall_score": round(self.overall_score, 4),
            "grade": self.grade,
            "details": self.details,
        }


def _grade(score: float) -> str:
    if score >= 0.95:
        return "A"
    if score >= 0.80:
        return "B"
    if score >= 0.65:
        return "C"
    if score >= 0.50:
        return "D"
    return "F"


def _validation_score(result: ValidationResult) -> float:
    """1.0 when fully valid, reduced by the fraction of errors."""
    if result.passed:
        return 1.0
    total_checks = max(len(result.errors), 1)
    # Each distinct error message counts as one failed check.
    return max(0.0, 1.0 - len(result.errors) / (total_checks + 1))


def _drift_score(report: Optional[DriftReport]) -> float:
    """1.0 when no drift, 0.0 when every column drifted."""
    if report is None:
        return 1.0
    if not report.has_drift:
        return 1.0
    total = len(report.drifted_columns) + len(report.missing_columns) + len(report.new_columns)
    if total == 0:
        return 1.0
    # Penalise proportionally; cap denominator to avoid division by zero.
    penalty = min(total / max(total, 1), 1.0)
    return round(max(0.0, 1.0 - penalty), 4)


def score_pipeline(
    validation_result: ValidationResult,
    drift_report: Optional[DriftReport] = None,
) -> QualityScore:
    """Compute an overall quality score for a pipeline run."""
    v_score = _validation_score(validation_result)
    d_score = _drift_score(drift_report)
    overall = _VALIDATION_WEIGHT * v_score + _DRIFT_WEIGHT * d_score

    details = {
        "validation_passed": validation_result.passed,
        "validation_errors": list(validation_result.errors),
        "drift_detected": drift_report.has_drift if drift_report else False,
    }

    return QualityScore(
        validation_score=v_score,
        drift_score=d_score,
        overall_score=round(overall, 4),
        grade=_grade(overall),
        details=details,
    )
