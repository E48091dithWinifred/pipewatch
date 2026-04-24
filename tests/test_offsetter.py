"""Tests for pipewatch.offsetter."""
from __future__ import annotations

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.offsetter import (
    OffsetConfig,
    OffsetResult,
    OffsetStatus,
    offset_all,
    offset_status,
)


def _make_status(
    name: str = "pipe_a",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.01,
    latency_ms: float = 120.0,
    message: str | None = None,
) -> PipelineStatus:
    return PipelineStatus(
        name=name,
        level=level,
        error_rate=error_rate,
        latency_ms=latency_ms,
        message=message,
    )


# --- OffsetConfig ---

def test_offset_config_defaults():
    cfg = OffsetConfig()
    assert cfg.error_rate_offset == 0.0
    assert cfg.latency_offset == 0.0
    assert cfg.clamp_min == 0.0


def test_offset_config_negative_clamp_raises():
    with pytest.raises(ValueError, match="clamp_min"):
        OffsetConfig(clamp_min=-1.0)


# --- offset_status ---

def test_offset_status_returns_offset_status_instance():
    s = _make_status()
    result = offset_status(s, OffsetConfig())
    assert isinstance(result, OffsetStatus)


def test_offset_status_name_passthrough():
    s = _make_status(name="my_pipe")
    result = offset_status(s, OffsetConfig())
    assert result.name == "my_pipe"


def test_offset_status_level_passthrough():
    s = _make_status(level=AlertLevel.WARNING)
    result = offset_status(s, OffsetConfig())
    assert result.level == "warning"


def test_offset_status_error_rate_shifted():
    s = _make_status(error_rate=0.05)
    result = offset_status(s, OffsetConfig(error_rate_offset=0.02))
    assert abs(result.error_rate - 0.07) < 1e-9


def test_offset_status_latency_shifted():
    s = _make_status(latency_ms=100.0)
    result = offset_status(s, OffsetConfig(latency_offset=50.0))
    assert abs(result.latency_ms - 150.0) < 1e-9


def test_offset_status_clamped_at_zero_by_default():
    s = _make_status(error_rate=0.01, latency_ms=10.0)
    result = offset_status(s, OffsetConfig(error_rate_offset=-0.5, latency_offset=-500.0))
    assert result.error_rate == 0.0
    assert result.latency_ms == 0.0


def test_offset_status_custom_clamp_min():
    s = _make_status(error_rate=0.0, latency_ms=0.0)
    result = offset_status(s, OffsetConfig(clamp_min=1.0))
    assert result.error_rate == 1.0
    assert result.latency_ms == 1.0


def test_offset_status_summary_contains_name():
    s = _make_status(name="etl_pipe")
    result = offset_status(s, OffsetConfig())
    assert "etl_pipe" in result.summary()


def test_offset_status_as_dict_has_expected_keys():
    s = _make_status()
    result = offset_status(s, OffsetConfig())
    d = result.as_dict()
    assert set(d.keys()) == {"name", "level", "error_rate", "latency_ms", "message"}


# --- offset_all ---

def test_offset_all_returns_offset_result():
    statuses = [_make_status(name=f"p{i}") for i in range(3)]
    result = offset_all(statuses)
    assert isinstance(result, OffsetResult)


def test_offset_all_count_matches_input():
    statuses = [_make_status(name=f"p{i}") for i in range(5)]
    result = offset_all(statuses)
    assert result.count == 5


def test_offset_all_empty_input():
    result = offset_all([])
    assert result.count == 0


def test_offset_all_summary_contains_count():
    statuses = [_make_status(name=f"p{i}") for i in range(4)]
    result = offset_all(statuses)
    assert "4" in result.summary()


def test_offset_all_applies_offset_to_each():
    statuses = [_make_status(error_rate=0.1), _make_status(error_rate=0.2)]
    result = offset_all(statuses, OffsetConfig(error_rate_offset=0.05))
    assert abs(result.statuses[0].error_rate - 0.15) < 1e-9
    assert abs(result.statuses[1].error_rate - 0.25) < 1e-9


def test_offset_all_uses_default_config_when_none():
    statuses = [_make_status(error_rate=0.3)]
    result = offset_all(statuses, None)
    assert abs(result.statuses[0].error_rate - 0.3) < 1e-9
