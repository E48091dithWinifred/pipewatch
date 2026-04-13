"""Tests for pipewatch.classifier."""
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.classifier import (
    ClassifiedPipeline,
    classify_all,
    classify_status,
)


def _make_status(
    name: str = "pipe",
    level: AlertLevel = AlertLevel.OK,
    message: str = "",
) -> PipelineStatus:
    return PipelineStatus(pipeline_name=name, level=level, message=message)


# --- classify_status ---

def test_classify_status_returns_classified_pipeline():
    result = classify_status(_make_status())
    assert isinstance(result, ClassifiedPipeline)


def test_classify_ok_is_healthy():
    result = classify_status(_make_status(level=AlertLevel.OK))
    assert result.category == "healthy"
    assert result.severity == 0
    assert result.hint is None


def test_classify_warning_error_rate():
    s = _make_status(level=AlertLevel.WARNING, message="error rate too high")
    result = classify_status(s)
    assert result.category == "error_rate"
    assert result.severity == 1
    assert result.hint is not None


def test_classify_critical_latency():
    s = _make_status(level=AlertLevel.CRITICAL, message="latency exceeded threshold")
    result = classify_status(s)
    assert result.category == "latency"
    assert result.severity == 2


def test_classify_throughput_message():
    s = _make_status(level=AlertLevel.WARNING, message="rows per second dropped")
    result = classify_status(s)
    assert result.category == "throughput"


def test_classify_general_fallback():
    s = _make_status(level=AlertLevel.WARNING, message="something went wrong")
    result = classify_status(s)
    assert result.category == "general"
    assert result.hint is not None


def test_is_actionable_ok_false():
    result = classify_status(_make_status(level=AlertLevel.OK))
    assert result.is_actionable is False


def test_is_actionable_warning_true():
    result = classify_status(_make_status(level=AlertLevel.WARNING, message="error spike"))
    assert result.is_actionable is True


def test_is_actionable_critical_true():
    result = classify_status(_make_status(level=AlertLevel.CRITICAL, message="latency"))
    assert result.is_actionable is True


# --- classify_all ---

def test_classify_all_empty_returns_empty():
    assert classify_all([]) == []


def test_classify_all_returns_correct_count():
    statuses = [
        _make_status("a", AlertLevel.OK),
        _make_status("b", AlertLevel.WARNING, "error rate"),
        _make_status("c", AlertLevel.CRITICAL, "latency"),
    ]
    results = classify_all(statuses)
    assert len(results) == 3


def test_classify_all_preserves_names():
    statuses = [
        _make_status("alpha", AlertLevel.OK),
        _make_status("beta", AlertLevel.CRITICAL, "latency"),
    ]
    names = [r.name for r in classify_all(statuses)]
    assert names == ["alpha", "beta"]


def test_classify_all_severity_ordering():
    statuses = [
        _make_status("x", AlertLevel.CRITICAL, "error"),
        _make_status("y", AlertLevel.WARNING, "latency"),
        _make_status("z", AlertLevel.OK),
    ]
    severities = [r.severity for r in classify_all(statuses)]
    assert severities == [2, 1, 0]
