"""Tests for pipewatch.hedger."""
from __future__ import annotations

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.hedger import HedgeConfig, HedgeResult, hedge_all, hedge_status


def _make_status(
    name: str = "pipe",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.0,
    latency_ms: float = 100.0,
) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        error_rate=error_rate,
        latency_ms=latency_ms,
        message="",
    )


@pytest.fixture
def default_cfg() -> HedgeConfig:
    return HedgeConfig(
        error_rate_margin=0.02,
        latency_margin_ms=50.0,
        warning_error_rate=0.05,
        critical_error_rate=0.10,
        warning_latency_ms=500.0,
        critical_latency_ms=1000.0,
    )


def test_hedge_status_returns_hedge_result(default_cfg):
    status = _make_status()
    result = hedge_status(status, default_cfg)
    assert isinstance(result, HedgeResult)


def test_hedge_ok_well_below_threshold_not_hedging(default_cfg):
    status = _make_status(error_rate=0.01, latency_ms=100.0)
    result = hedge_status(status, default_cfg)
    assert not result.is_hedging


def test_hedge_ok_near_warning_error_rate_is_hedging(default_cfg):
    # 0.04 is within 0.02 of warning threshold 0.05
    status = _make_status(error_rate=0.04, latency_ms=100.0)
    result = hedge_status(status, default_cfg)
    assert result.is_hedging


def test_hedge_ok_near_warning_latency_is_hedging(default_cfg):
    # 460 ms is within 50 ms of warning threshold 500 ms
    status = _make_status(error_rate=0.01, latency_ms=460.0)
    result = hedge_status(status, default_cfg)
    assert result.is_hedging


def test_hedge_warning_near_critical_error_rate_is_hedging(default_cfg):
    status = _make_status(
        level=AlertLevel.WARNING, error_rate=0.09, latency_ms=100.0
    )
    result = hedge_status(status, default_cfg)
    assert result.is_hedging


def test_hedge_critical_returns_none_margins(default_cfg):
    status = _make_status(
        level=AlertLevel.CRITICAL, error_rate=0.15, latency_ms=1200.0
    )
    result = hedge_status(status, default_cfg)
    assert result.error_rate_margin is None
    assert result.latency_margin is None


def test_hedge_critical_is_not_hedging(default_cfg):
    status = _make_status(
        level=AlertLevel.CRITICAL, error_rate=0.15, latency_ms=1200.0
    )
    result = hedge_status(status, default_cfg)
    # already critical — not hedging (no next threshold to approach)
    assert not result.is_hedging


def test_hedge_result_summary_contains_name(default_cfg):
    status = _make_status(name="my_pipe", error_rate=0.04)
    result = hedge_status(status, default_cfg)
    assert "my_pipe" in result.summary()


def test_hedge_result_summary_contains_hedging_flag(default_cfg):
    status = _make_status(error_rate=0.04)
    result = hedge_status(status, default_cfg)
    assert "HEDGING" in result.summary()


def test_hedge_result_summary_stable_when_not_hedging(default_cfg):
    status = _make_status(error_rate=0.01)
    result = hedge_status(status, default_cfg)
    assert "stable" in result.summary()


def test_hedge_all_returns_one_result_per_status(default_cfg):
    statuses = [
        _make_status("a"),
        _make_status("b"),
        _make_status("c"),
    ]
    results = hedge_all(statuses, default_cfg)
    assert len(results) == 3
    assert [r.name for r in results] == ["a", "b", "c"]


def test_hedge_all_empty_returns_empty(default_cfg):
    assert hedge_all([], default_cfg) == []


def test_hedge_uses_default_config_when_none():
    status = _make_status(error_rate=0.04, latency_ms=100.0)
    result = hedge_status(status)  # no cfg passed
    assert isinstance(result, HedgeResult)
