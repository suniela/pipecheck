"""Tests for pipecheck.threshold module."""
import pytest
from pipecheck.threshold import ThresholdConfig, ThresholdResult, check_thresholds


class TestThresholdConfig:
    def test_defaults_are_valid(self):
        cfg = ThresholdConfig()
        assert cfg.min_quality_score == 80.0
        assert cfg.max_null_rate == 0.10
        assert cfg.max_drift_columns == 0
        assert cfg.max_validation_errors == 0

    def test_invalid_quality_score_raises(self):
        with pytest.raises(ValueError, match="min_quality_score"):
            ThresholdConfig(min_quality_score=150.0)

    def test_invalid_null_rate_raises(self):
        with pytest.raises(ValueError, match="max_null_rate"):
            ThresholdConfig(max_null_rate=1.5)

    def test_negative_drift_columns_raises(self):
        with pytest.raises(ValueError, match="max_drift_columns"):
            ThresholdConfig(max_drift_columns=-1)

    def test_negative_validation_errors_raises(self):
        with pytest.raises(ValueError, match="max_validation_errors"):
            ThresholdConfig(max_validation_errors=-3)


class TestThresholdResult:
    def test_passed_summary(self):
        result = ThresholdResult(passed=True)
        assert result.summary() == "All thresholds passed."

    def test_failed_summary_lists_violations(self):
        result = ThresholdResult(passed=False, violations=["Score too low", "Null rate exceeded"])
        summary = result.summary()
        assert "Score too low" in summary
        assert "Null rate exceeded" in summary

    def test_to_dict_structure(self):
        result = ThresholdResult(passed=False, violations=["x"])
        d = result.to_dict()
        assert d["passed"] is False
        assert d["violations"] == ["x"]


class TestCheckThresholds:
    def test_all_passing(self):
        cfg = ThresholdConfig(min_quality_score=70.0, max_null_rate=0.2)
        result = check_thresholds(cfg, quality_score=90.0, null_rate=0.05)
        assert result.passed is True
        assert result.violations == []

    def test_quality_score_violation(self):
        cfg = ThresholdConfig(min_quality_score=80.0)
        result = check_thresholds(cfg, quality_score=60.0)
        assert result.passed is False
        assert any("Quality score" in v for v in result.violations)

    def test_null_rate_violation(self):
        cfg = ThresholdConfig(max_null_rate=0.05)
        result = check_thresholds(cfg, null_rate=0.20)
        assert result.passed is False
        assert any("Null rate" in v for v in result.violations)

    def test_drift_columns_violation(self):
        cfg = ThresholdConfig(max_drift_columns=0)
        result = check_thresholds(cfg, drift_columns=3)
        assert result.passed is False
        assert any("Drift" in v for v in result.violations)

    def test_validation_errors_violation(self):
        cfg = ThresholdConfig(max_validation_errors=0)
        result = check_thresholds(cfg, validation_errors=2)
        assert result.passed is False
        assert any("validation error" in v for v in result.violations)

    def test_multiple_violations(self):
        cfg = ThresholdConfig(min_quality_score=90.0, max_null_rate=0.01)
        result = check_thresholds(cfg, quality_score=50.0, null_rate=0.15)
        assert len(result.violations) == 2

    def test_none_metrics_are_skipped(self):
        cfg = ThresholdConfig()
        result = check_thresholds(cfg)
        assert result.passed is True
