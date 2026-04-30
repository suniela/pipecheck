"""Alerting module: evaluate pipeline results against thresholds and emit alerts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.scorer import QualityScore
from pipecheck.threshold import ThresholdConfig, ThresholdResult, check_threshold


@dataclass
class Alert:
    """A single alert message produced when a threshold is breached."""

    level: str  # "error" | "warning"
    message: str

    def to_dict(self) -> dict:
        return {"level": self.level, "message": self.message}


@dataclass
class AlertReport:
    """Collection of alerts produced for a pipeline run."""

    alerts: List[Alert] = field(default_factory=list)
    threshold_result: Optional[ThresholdResult] = None

    @property
    def has_alerts(self) -> bool:
        return len(self.alerts) > 0

    @property
    def has_errors(self) -> bool:
        return any(a.level == "error" for a in self.alerts)

    def summary(self) -> str:
        if not self.has_alerts:
            return "No alerts triggered."
        lines = [f"[{a.level.upper()}] {a.message}" for a in self.alerts]
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "has_alerts": self.has_alerts,
            "has_errors": self.has_errors,
            "alerts": [a.to_dict() for a in self.alerts],
            "threshold_result": self.threshold_result.to_dict() if self.threshold_result else None,
        }


def evaluate_alerts(
    score: QualityScore,
    config: ThresholdConfig,
) -> AlertReport:
    """Evaluate a QualityScore against a ThresholdConfig and return an AlertReport."""
    result: ThresholdResult = check_threshold(score, config)
    alerts: List[Alert] = []

    if score.overall < config.min_quality_score:
        alerts.append(
            Alert(
                level="error",
                message=(
                    f"Quality score {score.overall:.2f} is below minimum "
                    f"{config.min_quality_score:.2f} (grade: {score.grade})"
                ),
            )
        )

    if score.drift_report is not None and score.drift_report.has_drift:
        drifted = len(score.drift_report.drifted_columns)
        if drifted > config.max_drift_columns:
            alerts.append(
                Alert(
                    level="error",
                    message=(
                        f"{drifted} column(s) drifted, exceeding max allowed "
                        f"{config.max_drift_columns}"
                    ),
                )
            )
        else:
            alerts.append(
                Alert(
                    level="warning",
                    message=f"{drifted} column(s) drifted: {', '.join(score.drift_report.drifted_columns)}",
                )
            )

    return AlertReport(alerts=alerts, threshold_result=result)
