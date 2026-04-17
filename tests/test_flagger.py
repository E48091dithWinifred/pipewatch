"""Tests for pipewatch/flagger.py"""
import pytest
from pipewatch.checker import PipelineStatus, AlertLevel
from pipewatch.flagger import FlagConfig, FlaggedStatus, flag_status, flag_all
import datetime


def _make_status(
    name="pipe",
    level=AlertLevel.OK,
    error_rate=0.0,
    latency_ms=100.0,
    checked_at=None,
):
    s = PipelineStatus(pipeline_name=name, level=level, message="ok")
    s.error_rate = error_rate
    s.latency_ms = latency_ms
    s.checked_at = checked_at or datetime.datetime.utcnow().isoformat()
    return s


def test_flag_status_no_issues_returns_no_flags():
    s = _make_status()
    result = flag_status(s)
    assert result.flags == []


def test_flag_status_critical_adds_flag():
    s = _make_status(level=AlertLevel.CRITICAL)
    result = flag_status(s)
    assert "critical" in result.flags


def test_flag_status_high_error_rate_adds_flag():
    s = _make_status(error_rate=0.10)
    result = flag_status(s)
    assert "high_error_rate" in result.flags


def test_flag_status_below_error_rate_threshold_no_flag():
    s = _make_status(error_rate=0.01)
    result = flag_status(s)
    assert "high_error_rate" not in result.flags


def test_flag_status_slow_adds_flag():
    s = _make_status(latency_ms=9999.0)
    result = flag_status(s)
    assert "slow" in result.flags


def test_flag_status_fast_no_slow_flag():
    s = _make_status(latency_ms=50.0)
    result = flag_status(s)
    assert "slow" not in result.flags


def test_flag_status_stale_adds_flag():
    old_ts = (datetime.datetime.utcnow() - datetime.timedelta(seconds=600)).isoformat()
    s = _make_status(checked_at=old_ts)
    result = flag_status(s)
    assert "stale" in result.flags


def test_flag_status_recent_no_stale_flag():
    s = _make_status()
    result = flag_status(s)
    assert "stale" not in result.flags


def test_flag_status_is_flagged_true_when_flags_present():
    s = _make_status(level=AlertLevel.CRITICAL)
    result = flag_status(s)
    assert result.is_flagged is True


def test_flag_status_is_flagged_false_when_clean():
    s = _make_status()
    result = flag_status(s)
    assert result.is_flagged is False


def test_flag_status_summary_contains_name():
    s = _make_status(name="mypipe", level=AlertLevel.CRITICAL)
    result = flag_status(s)
    assert "mypipe" in result.summary()


def test_flag_status_summary_no_flags_message():
    s = _make_status(name="clean")
    result = flag_status(s)
    assert "no flags" in result.summary()


def test_flag_all_returns_list_of_flagged():
    statuses = [_make_status(name=f"p{i}") for i in range(3)]
    results = flag_all(statuses)
    assert len(results) == 3
    assert all(isinstance(r, FlaggedStatus) for r in results)


def test_flag_all_empty_returns_empty():
    assert flag_all([]) == []


def test_flag_config_disable_critical_flag():
    cfg = FlagConfig(flag_critical=False)
    s = _make_status(level=AlertLevel.CRITICAL)
    result = flag_status(s, config=cfg)
    assert "critical" not in result.flags
