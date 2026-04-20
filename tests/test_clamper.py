"""Tests for pipewatch.clamper."""
from __future__ import annotations

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.clamper import (
    ClampConfig,
    ClampResult,
    ClampedStatus,
    clamp_status,
    clamp_statuses,
)


def _make_status(
    name: str = "pipe",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.0,
    latency_ms: float = 100.0,
) -> PipelineStatus:
    return PipelineStatus(
        name=name,
        level=level,
        message="",
        error_rate=error_rate,
        latency_ms=latency_ms,
    )


# ── ClampConfig validation ───────────────────────────────────────────────────

def test_clamp_config_defaults():
    cfg = ClampConfig()
    assert cfg.min_error_rate == 0.0
    assert cfg.max_error_rate == 1.0


def test_clamp_config_invalid_error_rate_raises():
    with pytest.raises(ValueError, match="min_error_rate"):
        ClampConfig(min_error_rate=0.9, max_error_rate=0.1)


def test_clamp_config_invalid_latency_raises():
    with pytest.raises(ValueError, match="min_latency_ms"):
        ClampConfig(min_latency_ms=5000.0, max_latency_ms=100.0)


# ── clamp_status ─────────────────────────────────────────────────────────────

def test_clamp_status_no_clamping_needed():
    cfg = ClampConfig(min_error_rate=0.0, max_error_rate=1.0)
    s = _make_status(error_rate=0.5, latency_ms=200.0)
    result = clamp_status(s, cfg)
    assert result.error_rate == 0.5
    assert result.latency_ms == 200.0
    assert result.clamped_fields == []


def test_clamp_status_error_rate_above_max():
    cfg = ClampConfig(max_error_rate=0.5)
    s = _make_status(error_rate=0.9)
    result = clamp_status(s, cfg)
    assert result.error_rate == 0.5
    assert "error_rate" in result.clamped_fields


def test_clamp_status_error_rate_below_min():
    cfg = ClampConfig(min_error_rate=0.1)
    s = _make_status(error_rate=0.0)
    result = clamp_status(s, cfg)
    assert result.error_rate == 0.1
    assert "error_rate" in result.clamped_fields


def test_clamp_status_latency_above_max():
    cfg = ClampConfig(max_latency_ms=500.0)
    s = _make_status(latency_ms=9999.0)
    result = clamp_status(s, cfg)
    assert result.latency_ms == 500.0
    assert "latency_ms" in result.clamped_fields


def test_clamp_status_latency_below_min():
    cfg = ClampConfig(min_latency_ms=50.0)
    s = _make_status(latency_ms=10.0)
    result = clamp_status(s, cfg)
    assert result.latency_ms == 50.0
    assert "latency_ms" in result.clamped_fields


def test_clamp_status_name_and_level_preserved():
    s = _make_status(name="my_pipe", level=AlertLevel.WARNING)
    result = clamp_status(s, ClampConfig())
    assert result.name == "my_pipe"
    assert result.level == AlertLevel.WARNING


def test_clamp_status_was_clamped_true():
    cfg = ClampConfig(max_error_rate=0.3)
    s = _make_status(error_rate=0.8)
    result = clamp_status(s, cfg)
    assert result.was_clamped() is True


def test_clamp_status_was_clamped_false():
    s = _make_status(error_rate=0.1)
    result = clamp_status(s, ClampConfig())
    assert result.was_clamped() is False


def test_clamp_status_summary_clamped():
    cfg = ClampConfig(max_error_rate=0.2)
    s = _make_status(name="alpha", error_rate=0.9)
    result = clamp_status(s, cfg)
    assert "alpha" in result.summary()
    assert "error_rate" in result.summary()


def test_clamp_status_summary_no_clamp():
    s = _make_status(name="beta")
    result = clamp_status(s, ClampConfig())
    assert "no clamping" in result.summary()


# ── clamp_statuses ────────────────────────────────────────────────────────────

def test_clamp_statuses_returns_clamp_result():
    statuses = [_make_status(), _make_status(name="b")]
    result = clamp_statuses(statuses)
    assert isinstance(result, ClampResult)


def test_clamp_statuses_total():
    statuses = [_make_status(name=f"p{i}") for i in range(5)]
    result = clamp_statuses(statuses)
    assert result.total == 5


def test_clamp_statuses_clamped_count():
    cfg = ClampConfig(max_error_rate=0.1)
    statuses = [
        _make_status(name="a", error_rate=0.5),
        _make_status(name="b", error_rate=0.05),
        _make_status(name="c", error_rate=0.8),
    ]
    result = clamp_statuses(statuses, cfg)
    assert result.clamped_count == 2


def test_clamp_statuses_default_config():
    statuses = [_make_status(error_rate=0.3, latency_ms=200.0)]
    result = clamp_statuses(statuses)
    assert result.clamped_count == 0


def test_clamp_statuses_summary_string():
    cfg = ClampConfig(max_latency_ms=100.0)
    statuses = [_make_status(latency_ms=9000.0)]
    result = clamp_statuses(statuses, cfg)
    assert "1/1" in result.summary()
