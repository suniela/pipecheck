"""Tests for pipecheck.scorer."""
from __future__ import annotations

import pytest

from pipecheck.scorer import QualityScore, _grade, score_pipeline
from pipecheck.validator import ValidationResult
from pipecheck.watcher import DriftReport


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _passing_result() -> ValidationResult:
    return ValidationResult(passed=True, errors=[])


def _failing_result(*errors: str) -> ValidationResult:
    return ValidationResult(passed=False, errors=list(errors))


def _no_drift_report() -> DriftReport:
    return DriftReport(drifted_columns=[], missing_columns=[], new_columns=[])


def _drift_report(*drifted: str) -> DriftReport:
    return DriftReport(drifted_columns=list(drifted), missing_columns=[], new_columns=[])


# ---------------------------------------------------------------------------
# _grade
# ---------------------------------------------------------------------------

class TestGrade:
    def test_a_at_095(self):
        assert _grade(0.95) == "A"

    def test_b_at_080(self):
        assert _grade(0.80) == "B"

    def test_c_at_065(self):
        assert _grade(0.65) == "C"

    def test_d_at_050(self):
        assert _grade(0.50) == "D"

    def test_f_below_050(self):
        assert _grade(0.49) == "F"


# ---------------------------------------------------------------------------
# score_pipeline
# ---------------------------------------------------------------------------

class TestScorePipeline:
    def test_perfect_score_no_drift(self):
        result = score_pipeline(_passing_result(), _no_drift_report())
        assert result.overall_score == 1.0
        assert result.grade == "A"

    def test_perfect_score_without_drift_report(self):
        result = score_pipeline(_passing_result(), None)
        assert result.overall_score == 1.0

    def test_validation_failure_lowers_score(self):
        result = score_pipeline(_failing_result("col_a missing"), _no_drift_report())
        assert result.overall_score < 1.0
        assert result.validation_score < 1.0
        assert result.drift_score == 1.0

    def test_drift_lowers_score(self):
        result = score_pipeline(_passing_result(), _drift_report("col_a"))
        assert result.drift_score < 1.0
        assert result.overall_score < 1.0

    def test_both_failures_produce_lowest_score(self):
        bad_valid = _failing_result("e1", "e2", "e3")
        bad_drift = _drift_report("col_a", "col_b", "col_c")
        result = score_pipeline(bad_valid, bad_drift)
        assert result.overall_score < 0.8

    def test_score_is_between_0_and_1(self):
        result = score_pipeline(_failing_result("err"), _drift_report("x"))
        assert 0.0 <= result.overall_score <= 1.0

    def test_to_dict_keys(self):
        result = score_pipeline(_passing_result())
        d = result.to_dict()
        assert set(d.keys()) == {"validation_score", "drift_score", "overall_score", "grade", "details"}

    def test_details_contain_expected_fields(self):
        result = score_pipeline(_passing_result(), _no_drift_report())
        assert "validation_passed" in result.details
        assert "drift_detected" in result.details
        assert "validation_errors" in result.details
