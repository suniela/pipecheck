"""Tests for pipecheck.alerting."""

import pytest

from pipecheck.alerting import Alert, AlertReport, evaluate_alerts
from pipecheck.scorer import QualityScore
from pipecheck.threshold import ThresholdConfig
from pipecheck.validator import ValidationResult
from pipecheck.watcher import DriftReport


def _passing_result() -> ValidationResult:
    return ValidationResult(passed=True, errors=[])


def _failing_result() -> ValidationResult:
    return ValidationResult(passed=False, errors=["col 'id' missing"])


def _no_drift() -> DriftReport:
    return DriftReport(drifted_columns=[], messages=[])


def _drift(cols=("price",)) -> DriftReport:
    return DriftReport(
        drifted_columns=list(cols),
        messages=[f"{c} drifted" for c in cols],
    )


class TestAlertProperties:
    def test_to_dict_keys(self):
        a = Alert(level="error", message="bad")
        d = a.to_dict()
        assert set(d.keys()) == {"level", "message"}


class TestAlertReport:
    def test_no_alerts_has_alerts_false(self):
        report = AlertReport()
        assert not report.has_alerts

    def test_with_alerts_has_alerts_true(self):
        report = AlertReport(alerts=[Alert("warning", "drift detected")])
        assert report.has_alerts

    def test_has_errors_only_for_error_level(self):
        report = AlertReport(alerts=[Alert("warning", "minor issue")])
        assert not report.has_errors

    def test_has_errors_true_when_error_present(self):
        report = AlertReport(alerts=[Alert("error", "critical failure")])
        assert report.has_errors

    def test_summary_no_alerts(self):
        assert AlertReport().summary() == "No alerts triggered."

    def test_summary_lists_alerts(self):
        report = AlertReport(alerts=[Alert("error", "bad score")])
        assert "[ERROR]" in report.summary()
        assert "bad score" in report.summary()

    def test_to_dict_structure(self):
        report = AlertReport(alerts=[Alert("warning", "drift")])
        d = report.to_dict()
        assert "alerts" in d
        assert d["has_alerts"] is True


class TestEvaluateAlerts:
    def test_no_alerts_when_score_passes(self):
        score = QualityScore.compute(
            validation_result=_passing_result(),
            drift_report=_no_drift(),
        )
        config = ThresholdConfig(min_quality_score=0.0)
        report = evaluate_alerts(score, config)
        assert not report.has_alerts

    def test_error_when_score_below_threshold(self):
        score = QualityScore.compute(
            validation_result=_failing_result(),
            drift_report=_no_drift(),
        )
        config = ThresholdConfig(min_quality_score=0.99)
        report = evaluate_alerts(score, config)
        assert report.has_errors
        assert any("below minimum" in a.message for a in report.alerts)

    def test_warning_when_drift_within_limit(self):
        score = QualityScore.compute(
            validation_result=_passing_result(),
            drift_report=_drift(("price",)),
        )
        config = ThresholdConfig(min_quality_score=0.0, max_drift_columns=5)
        report = evaluate_alerts(score, config)
        assert report.has_alerts
        assert any(a.level == "warning" for a in report.alerts)

    def test_error_when_drift_exceeds_limit(self):
        score = QualityScore.compute(
            validation_result=_passing_result(),
            drift_report=_drift(("a", "b", "c")),
        )
        config = ThresholdConfig(min_quality_score=0.0, max_drift_columns=1)
        report = evaluate_alerts(score, config)
        assert report.has_errors
        assert any("exceeding max" in a.message for a in report.alerts)

    def test_threshold_result_attached(self):
        score = QualityScore.compute(
            validation_result=_passing_result(),
            drift_report=_no_drift(),
        )
        config = ThresholdConfig()
        report = evaluate_alerts(score, config)
        assert report.threshold_result is not None
