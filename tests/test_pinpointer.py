import pytest
from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.pinpointer import PinpointResult, pinpoint, _score


def _make_status(name, level, error_rate=0.0, latency_ms=0.0):
    return PipelineStatus(
        name=name,
        level=level,
        message="",
        error_rate=error_rate,
        latency_ms=latency_ms,
    )


def test_pinpoint_empty_returns_none():
    result = pinpoint([])
    assert result.pipeline is None
    assert result.score == 0.0


def test_pinpoint_all_ok_returns_none():
    statuses = [
        _make_status("a", AlertLevel.OK),
        _make_status("b", AlertLevel.OK),
    ]
    result = pinpoint(statuses)
    assert result.pipeline is None


def test_pinpoint_returns_critical_over_warning():
    statuses = [
        _make_status("warn", AlertLevel.WARNING, error_rate=0.1),
        _make_status("crit", AlertLevel.CRITICAL, error_rate=0.05),
    ]
    result = pinpoint(statuses)
    assert result.pipeline is not None
    assert result.pipeline.name == "crit"


def test_pinpoint_returns_highest_error_rate_among_same_level():
    statuses = [
        _make_status("low", AlertLevel.WARNING, error_rate=0.05),
        _make_status("high", AlertLevel.WARNING, error_rate=0.30),
    ]
    result = pinpoint(statuses)
    assert result.pipeline.name == "high"


def test_pinpoint_result_is_pinpoint_result_instance():
    statuses = [_make_status("x", AlertLevel.CRITICAL, error_rate=0.2)]
    result = pinpoint(statuses)
    assert isinstance(result, PinpointResult)


def test_pinpoint_reason_contains_level():
    statuses = [_make_status("p", AlertLevel.CRITICAL, error_rate=0.1)]
    result = pinpoint(statuses)
    assert "critical" in result.reason


def test_pinpoint_reason_contains_error_rate():
    statuses = [_make_status("p", AlertLevel.WARNING, error_rate=0.25)]
    result = pinpoint(statuses)
    assert "error_rate" in result.reason


def test_pinpoint_summary_contains_name():
    statuses = [_make_status("my_pipeline", AlertLevel.CRITICAL)]
    result = pinpoint(statuses)
    assert "my_pipeline" in result.summary()


def test_pinpoint_summary_no_pipeline():
    result = PinpointResult(pipeline=None, reason="empty input", score=0.0)
    assert "No problematic" in result.summary()


def test_score_critical_higher_than_ok():
    ok = _make_status("a", AlertLevel.OK)
    crit = _make_status("b", AlertLevel.CRITICAL)
    assert _score(crit) > _score(ok)


def test_pinpoint_single_warning_is_returned():
    statuses = [_make_status("only", AlertLevel.WARNING, latency_ms=500.0)]
    result = pinpoint(statuses)
    assert result.pipeline.name == "only"
