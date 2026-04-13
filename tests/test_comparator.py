"""Tests for pipewatch.comparator."""
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.comparator import (
    ComparisonReport,
    PipelineChange,
    compare_runs,
)


def _make_status(
    name: str,
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.0,
    message: str = "",
) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        error_rate=error_rate,
        latency_ms=100.0,
        message=message,
    )


# ---------------------------------------------------------------------------
# PipelineChange helpers
# ---------------------------------------------------------------------------

def test_pipeline_change_degraded():
    c = PipelineChange(
        name="p",
        previous_level=AlertLevel.OK,
        current_level=AlertLevel.WARNING,
        previous_error_rate=0.01,
        current_error_rate=0.05,
    )
    assert c.degraded is True
    assert c.improved is False


def test_pipeline_change_improved():
    c = PipelineChange(
        name="p",
        previous_level=AlertLevel.CRITICAL,
        current_level=AlertLevel.OK,
        previous_error_rate=0.2,
        current_error_rate=0.01,
    )
    assert c.improved is True
    assert c.degraded is False


def test_pipeline_change_error_rate_delta():
    c = PipelineChange(
        name="p",
        previous_level=AlertLevel.OK,
        current_level=AlertLevel.WARNING,
        previous_error_rate=0.02,
        current_error_rate=0.07,
    )
    assert abs(c.error_rate_delta - 0.05) < 1e-9


# ---------------------------------------------------------------------------
# compare_runs
# ---------------------------------------------------------------------------

def test_compare_runs_no_changes():
    prev = [_make_status("a"), _make_status("b")]
    curr = [_make_status("a"), _make_status("b")]
    report = compare_runs(prev, curr)
    assert report.changes == []
    assert report.added == []
    assert report.removed == []


def test_compare_runs_detects_added():
    prev = [_make_status("a")]
    curr = [_make_status("a"), _make_status("new")]
    report = compare_runs(prev, curr)
    assert "new" in report.added


def test_compare_runs_detects_removed():
    prev = [_make_status("a"), _make_status("gone")]
    curr = [_make_status("a")]
    report = compare_runs(prev, curr)
    assert "gone" in report.removed


def test_compare_runs_detects_level_change():
    prev = [_make_status("a", AlertLevel.OK, 0.01)]
    curr = [_make_status("a", AlertLevel.CRITICAL, 0.25)]
    report = compare_runs(prev, curr)
    assert len(report.changes) == 1
    assert report.changes[0].degraded is True


def test_compare_runs_has_regressions_true():
    prev = [_make_status("a", AlertLevel.OK)]
    curr = [_make_status("a", AlertLevel.WARNING, 0.1)]
    report = compare_runs(prev, curr)
    assert report.has_regressions is True


def test_compare_runs_has_regressions_false():
    prev = [_make_status("a", AlertLevel.WARNING, 0.1)]
    curr = [_make_status("a", AlertLevel.OK, 0.01)]
    report = compare_runs(prev, curr)
    assert report.has_regressions is False


def test_compare_runs_improved_list():
    prev = [_make_status("a", AlertLevel.CRITICAL, 0.3)]
    curr = [_make_status("a", AlertLevel.OK, 0.01)]
    report = compare_runs(prev, curr)
    assert len(report.improved) == 1
    assert report.improved[0].name == "a"
