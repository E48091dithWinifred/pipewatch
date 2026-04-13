"""Tests for pipewatch.validator."""
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.validator import (
    ValidationRule,
    ValidationViolation,
    ValidationReport,
    validate_status,
    validate_all,
)


def _make_status(
    name: str = "pipe",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.0,
    latency_ms: float = 100.0,
) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message="ok",
        error_rate=error_rate,
        latency_ms=latency_ms,
    )


@pytest.fixture
def basic_rule() -> ValidationRule:
    return ValidationRule(
        name="basic",
        max_error_rate=0.05,
        max_latency_ms=500.0,
        forbidden_levels=["critical"],
    )


def test_validate_status_passes_when_all_ok(basic_rule):
    status = _make_status(error_rate=0.01, latency_ms=200.0)
    report = validate_status(status, [basic_rule])
    assert report.passed
    assert report.violations == []


def test_validate_status_violation_on_high_error_rate(basic_rule):
    status = _make_status(error_rate=0.10)
    report = validate_status(status, [basic_rule])
    assert not report.passed
    assert any("error_rate" in v.message for v in report.violations)


def test_validate_status_violation_on_high_latency(basic_rule):
    status = _make_status(latency_ms=1000.0)
    report = validate_status(status, [basic_rule])
    assert not report.passed
    assert any("latency_ms" in v.message for v in report.violations)


def test_validate_status_violation_on_forbidden_level(basic_rule):
    status = _make_status(level=AlertLevel.CRITICAL)
    report = validate_status(status, [basic_rule])
    assert not report.passed
    assert any("forbidden" in v.message for v in report.violations)


def test_validate_status_multiple_violations(basic_rule):
    status = _make_status(
        level=AlertLevel.CRITICAL, error_rate=0.20, latency_ms=2000.0
    )
    report = validate_status(status, [basic_rule])
    assert len(report.violations) == 3


def test_validation_violation_summary():
    v = ValidationViolation(pipeline="p1", rule="r1", message="too slow")
    assert "p1" in v.summary
    assert "r1" in v.summary
    assert "too slow" in v.summary


def test_validate_all_returns_one_report_per_status(basic_rule):
    statuses = [_make_status(name=f"pipe_{i}") for i in range(4)]
    reports = validate_all(statuses, [basic_rule])
    assert len(reports) == 4


def test_validate_all_mixed_results(basic_rule):
    statuses = [
        _make_status(name="ok_pipe", error_rate=0.01),
        _make_status(name="bad_pipe", error_rate=0.99),
    ]
    reports = validate_all(statuses, [basic_rule])
    passing = [r for r in reports if r.passed]
    failing = [r for r in reports if not r.passed]
    assert len(passing) == 1
    assert len(failing) == 1


def test_validate_status_no_rules():
    status = _make_status(level=AlertLevel.CRITICAL, error_rate=1.0)
    report = validate_status(status, [])
    assert report.passed


def test_validate_status_pipeline_name_in_report(basic_rule):
    status = _make_status(name="my_pipeline")
    report = validate_status(status, [basic_rule])
    assert report.pipeline == "my_pipeline"
