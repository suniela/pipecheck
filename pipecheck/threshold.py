"""Threshold enforcement for quality scores and validation metrics."""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ThresholdConfig:
    """Defines pass/fail thresholds for pipeline quality gates."""
    min_quality_score: float = 80.0
    max_null_rate: float = 0.10
    max_drift_columns: int = 0
    max_validation_errors: int = 0

    def __post_init__(self):
        if not (0.0 <= self.min_quality_score <= 100.0):
            raise ValueError("min_quality_score must be between 0 and 100")
        if not (0.0 <= self.max_null_rate <= 1.0):
            raise ValueError("max_null_rate must be between 0 and 1")
        if self.max_drift_columns < 0:
            raise ValueError("max_drift_columns must be >= 0")
        if self.max_validation_errors < 0:
            raise ValueError("max_validation_errors must be >= 0")


@dataclass
class ThresholdResult:
    """Outcome of evaluating a ThresholdConfig against pipeline metrics."""
    passed: bool
    violations: list = field(default_factory=list)

    def summary(self) -> str:
        if self.passed:
            return "All thresholds passed."
        lines = ["Threshold violations:"]
        for v in self.violations:
            lines.append(f"  - {v}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {"passed": self.passed, "violations": self.violations}


def check_thresholds(
    config: ThresholdConfig,
    quality_score: Optional[float] = None,
    null_rate: Optional[float] = None,
    drift_columns: Optional[int] = None,
    validation_errors: Optional[int] = None,
) -> ThresholdResult:
    """Evaluate pipeline metrics against the given ThresholdConfig."""
    violations = []

    if quality_score is not None and quality_score < config.min_quality_score:
        violations.append(
            f"Quality score {quality_score:.1f} is below minimum {config.min_quality_score:.1f}"
        )

    if null_rate is not None and null_rate > config.max_null_rate:
        violations.append(
            f"Null rate {null_rate:.2%} exceeds maximum {config.max_null_rate:.2%}"
        )

    if drift_columns is not None and drift_columns > config.max_drift_columns:
        violations.append(
            f"Drift detected in {drift_columns} column(s), maximum allowed is {config.max_drift_columns}"
        )

    if validation_errors is not None and validation_errors > config.max_validation_errors:
        violations.append(
            f"{validation_errors} validation error(s) found, maximum allowed is {config.max_validation_errors}"
        )

    return ThresholdResult(passed=len(violations) == 0, violations=violations)
