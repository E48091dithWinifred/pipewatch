"""Tests for pipewatch.reaper."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.reaper import ReaperConfig, ReapResult, reap_statuses, _age_seconds


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _make_status(name: str, minutes_ago: float) -> PipelineStatus:
    ts = (NOW - timedelta(minutes=minutes_ago)).isoformat()
    return PipelineStatus(
        pipeline_name=name,
        level=AlertLevel.OK,
        message="ok",
        error_rate=0.0,
        latency_ms=100.0,
        checked_at=ts,
    )


def test_age_seconds_recent():
    ts = (NOW - timedelta(seconds=30)).isoformat()
    age = _age_seconds(ts, NOW)
    assert abs(age - 30.0) < 1.0


def test_age_seconds_invalid_returns_inf():
    assert _age_seconds("not-a-date", NOW) == float("inf")


def test_age_seconds_none_returns_inf():
    assert _age_seconds(None, NOW) == float("inf")


def test_reap_result_counts():
    r = ReapResult(kept=[object()], reaped=[object(), object()])
    assert r.kept_count == 1
    assert r.reaped_count == 2


def test_reap_result_summary():
    r = ReapResult(kept=[object()], reaped=[object()])
    assert "reaped=1" in r.summary()
    assert "kept=1" in r.summary()


def test_reap_empty_returns_empty():
    result = reap_statuses([], now=NOW)
    assert result.kept == []
    assert result.reaped == []


def test_reap_fresh_status_is_kept():
    s = _make_status("pipe-a", minutes_ago=5)
    cfg = ReaperConfig(max_age_seconds=3600)
    result = reap_statuses([s], config=cfg, now=NOW)
    assert result.kept_count == 1
    assert result.reaped_count == 0


def test_reap_stale_status_is_reaped():
    s = _make_status("pipe-b", minutes_ago=90)
    cfg = ReaperConfig(max_age_seconds=3600)
    result = reap_statuses([s], config=cfg, now=NOW)
    assert result.reaped_count == 1
    assert result.kept_count == 0


def test_reap_mixed_statuses():
    fresh = _make_status("fresh", minutes_ago=10)
    stale = _make_status("stale", minutes_ago=120)
    cfg = ReaperConfig(max_age_seconds=3600)
    result = reap_statuses([fresh, stale], config=cfg, now=NOW)
    assert result.kept_count == 1
    assert result.reaped_count == 1
    assert result.kept[0].pipeline_name == "fresh"
    assert result.reaped[0].pipeline_name == "stale"


def test_reap_no_checked_at_is_reaped():
    s = PipelineStatus(
        pipeline_name="no-ts",
        level=AlertLevel.OK,
        message="ok",
        error_rate=0.0,
        latency_ms=50.0,
        checked_at=None,
    )
    result = reap_statuses([s], config=ReaperConfig(max_age_seconds=3600), now=NOW)
    assert result.reaped_count == 1


def test_reap_default_config_is_one_hour():
    fresh = _make_status("f", minutes_ago=30)
    stale = _make_status("s", minutes_ago=90)
    result = reap_statuses([fresh, stale], now=NOW)
    assert result.kept_count == 1
    assert result.reaped_count == 1
