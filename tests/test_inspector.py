"""Tests for pipewatch.inspector."""
from __future__ import annotations

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.config import AlertThresholds, PipelineConfig
from pipewatch.inspector import (
    InspectionFinding,
    InspectionReport,
    inspect_all,
    inspect_pipeline,
)


def _make_status(
    name: str = "pipe_a",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.0,
    latency_ms: float = 100.0,
) -> PipelineStatus:
    thresholds = AlertThresholds(
        error_rate_warning=0.05,
        error_rate_critical=0.15,
        latency_warning_ms=300.0,
        latency_critical_ms=600.0,
    )
    config = PipelineConfig(name=name, thresholds=thresholds)
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        error_rate=error_rate,
        latency_ms=latency_ms,
        message="",
        config=config,
    )


def test_inspect_pipeline_returns_report():
    status = _make_status()
    report = inspect_pipeline(status)
    assert isinstance(report, InspectionReport)


def test_inspect_pipeline_name_matches():
    status = _make_status(name="etl_load")
    report = inspect_pipeline(status)
    assert report.pipeline_name == "etl_load"


def test_inspect_pipeline_overall_level():
    status = _make_status(level=AlertLevel.WARNING)
    report = inspect_pipeline(status)
    assert report.overall_level == AlertLevel.WARNING


def test_inspect_pipeline_has_two_findings():
    status = _make_status()
    report = inspect_pipeline(status)
    assert len(report.findings) == 2


def test_inspect_ok_no_issues():
    status = _make_status(error_rate=0.01, latency_ms=50.0)
    report = inspect_pipeline(status)
    assert not report.has_issues()


def test_inspect_warning_error_rate_finding():
    status = _make_status(level=AlertLevel.WARNING, error_rate=0.08)
    report = inspect_pipeline(status)
    er_finding = next(f for f in report.findings if f.field_name == "error_rate")
    assert er_finding.severity == AlertLevel.WARNING
    assert er_finding.is_actionable()


def test_inspect_critical_error_rate_finding():
    status = _make_status(level=AlertLevel.CRITICAL, error_rate=0.20)
    report = inspect_pipeline(status)
    er_finding = next(f for f in report.findings if f.field_name == "error_rate")
    assert er_finding.severity == AlertLevel.CRITICAL


def test_inspect_warning_latency_finding():
    status = _make_status(level=AlertLevel.WARNING, latency_ms=400.0)
    report = inspect_pipeline(status)
    lat_finding = next(f for f in report.findings if f.field_name == "latency_ms")
    assert lat_finding.severity == AlertLevel.WARNING


def test_inspect_critical_latency_finding():
    status = _make_status(level=AlertLevel.CRITICAL, latency_ms=700.0)
    report = inspect_pipeline(status)
    lat_finding = next(f for f in report.findings if f.field_name == "latency_ms")
    assert lat_finding.severity == AlertLevel.CRITICAL


def test_actionable_filters_ok_findings():
    status = _make_status(level=AlertLevel.WARNING, error_rate=0.08, latency_ms=50.0)
    report = inspect_pipeline(status)
    actionable = report.actionable()
    assert all(f.is_actionable() for f in actionable)
    assert any(f.field_name == "error_rate" for f in actionable)


def test_finding_summary_contains_field_name():
    finding = InspectionFinding("error_rate", 0.08, "Too high", AlertLevel.WARNING)
    assert "error_rate" in finding.summary()


def test_report_summary_contains_pipeline_name():
    status = _make_status(name="my_pipe")
    report = inspect_pipeline(status)
    assert "my_pipe" in report.summary()


def test_inspect_all_returns_one_per_status():
    statuses = [_make_status(name=f"pipe_{i}") for i in range(4)]
    reports = inspect_all(statuses)
    assert len(reports) == 4
    names = [r.pipeline_name for r in reports]
    assert "pipe_0" in names and "pipe_3" in names


def test_inspect_all_empty_returns_empty():
    assert inspect_all([]) == []
