"""Tests for pipewatch.scaler."""
from __future__ import annotations

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.scaler import ScalerConfig, ScaledStatus, scale_status, scale_all


def _make_status(
    name: str = "pipe",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.0,
    latency_ms: float = 0.0,
) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        error_rate=error_rate,
        latency_ms=latency_ms,
        message="",
    )


# --- ScalerConfig ---

def test_scaler_config_defaults():
    cfg = ScalerConfig()
    assert cfg.max_error_rate == 1.0
    assert cfg.max_latency_ms == 10_000.0


def test_scaler_config_invalid_error_rate_raises():
    with pytest.raises(ValueError, match="max_error_rate"):
        ScalerConfig(max_error_rate=0.0)


def test_scaler_config_invalid_latency_raises():
    with pytest.raises(ValueError, match="max_latency_ms"):
        ScalerConfig(max_latency_ms=-1.0)


# --- scale_status ---

def test_scale_status_returns_scaled_instance():
    result = scale_status(_make_status())
    assert isinstance(result, ScaledStatus)


def test_scale_status_name_preserved():
    result = scale_status(_make_status(name="my_pipe"))
    assert result.name == "my_pipe"


def test_scale_status_level_preserved():
    result = scale_status(_make_status(level=AlertLevel.WARNING))
    assert result.level == "warning"


def test_scale_status_zero_metrics_gives_zero():
    result = scale_status(_make_status(error_rate=0.0, latency_ms=0.0))
    assert result.error_rate_scaled == 0.0
    assert result.latency_scaled == 0.0
    assert result.composite_score == 0.0


def test_scale_status_max_metrics_gives_one():
    cfg = ScalerConfig(max_error_rate=0.5, max_latency_ms=2000.0)
    result = scale_status(_make_status(error_rate=0.5, latency_ms=2000.0), cfg)
    assert result.error_rate_scaled == pytest.approx(1.0)
    assert result.latency_scaled == pytest.approx(1.0)
    assert result.composite_score == pytest.approx(1.0)


def test_scale_status_clamps_above_max():
    cfg = ScalerConfig(max_error_rate=0.1, max_latency_ms=100.0)
    result = scale_status(_make_status(error_rate=1.0, latency_ms=9999.0), cfg)
    assert result.error_rate_scaled == pytest.approx(1.0)
    assert result.latency_scaled == pytest.approx(1.0)


def test_scale_status_composite_is_average():
    cfg = ScalerConfig(max_error_rate=1.0, max_latency_ms=1000.0)
    result = scale_status(_make_status(error_rate=0.4, latency_ms=600.0), cfg)
    expected = (0.4 + 0.6) / 2.0
    assert result.composite_score == pytest.approx(expected)


def test_scale_status_summary_contains_name():
    result = scale_status(_make_status(name="alpha"))
    assert "alpha" in result.summary()


# --- scale_all ---

def test_scale_all_empty_returns_empty():
    assert scale_all([]) == []


def test_scale_all_returns_correct_count():
    statuses = [_make_status(name=f"p{i}") for i in range(5)]
    results = scale_all(statuses)
    assert len(results) == 5


def test_scale_all_uses_shared_config():
    cfg = ScalerConfig(max_error_rate=0.2, max_latency_ms=500.0)
    statuses = [
        _make_status(name="a", error_rate=0.2, latency_ms=500.0),
        _make_status(name="b", error_rate=0.1, latency_ms=250.0),
    ]
    results = scale_all(statuses, cfg)
    assert results[0].error_rate_scaled == pytest.approx(1.0)
    assert results[1].latency_scaled == pytest.approx(0.5)
