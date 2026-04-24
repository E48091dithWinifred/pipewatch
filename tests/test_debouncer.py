"""Tests for pipewatch.debouncer."""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.debouncer import (
    DebounceConfig,
    DebounceResult,
    debounce,
    _entry_key,
)


def _make_status(name: str, level: AlertLevel, error_rate: float = 0.0) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        error_rate=error_rate,
        latency_ms=100.0,
        message=f"{name} {level.value}",
    )


@pytest.fixture()
def state_file(tmp_path: Path) -> str:
    return str(tmp_path / "debounce_state.json")


def test_debounce_config_defaults() -> None:
    cfg = DebounceConfig()
    assert cfg.cooldown_seconds == 300


def test_debounce_config_negative_raises() -> None:
    with pytest.raises(ValueError):
        DebounceConfig(cooldown_seconds=-1)


def test_ok_status_always_allowed(state_file: str) -> None:
    cfg = DebounceConfig(cooldown_seconds=60, state_path=state_file)
    status = _make_status("pipe_a", AlertLevel.OK)
    result = debounce([status], cfg)
    assert len(result.allowed) == 1
    assert len(result.suppressed) == 0


def test_first_non_ok_is_allowed(state_file: str) -> None:
    cfg = DebounceConfig(cooldown_seconds=60, state_path=state_file)
    status = _make_status("pipe_a", AlertLevel.WARNING)
    result = debounce([status], cfg, _now=1000.0)
    assert status in result.allowed


def test_second_call_within_cooldown_is_suppressed(state_file: str) -> None:
    cfg = DebounceConfig(cooldown_seconds=60, state_path=state_file)
    status = _make_status("pipe_a", AlertLevel.WARNING)
    debounce([status], cfg, _now=1000.0)
    result = debounce([status], cfg, _now=1030.0)  # 30s later, within 60s cooldown
    assert status in result.suppressed


def test_second_call_after_cooldown_is_allowed(state_file: str) -> None:
    cfg = DebounceConfig(cooldown_seconds=60, state_path=state_file)
    status = _make_status("pipe_a", AlertLevel.WARNING)
    debounce([status], cfg, _now=1000.0)
    result = debounce([status], cfg, _now=1070.0)  # 70s later, past 60s cooldown
    assert status in result.allowed


def test_different_pipelines_tracked_independently(state_file: str) -> None:
    cfg = DebounceConfig(cooldown_seconds=60, state_path=state_file)
    a = _make_status("pipe_a", AlertLevel.CRITICAL)
    b = _make_status("pipe_b", AlertLevel.CRITICAL)
    debounce([a], cfg, _now=1000.0)
    result = debounce([a, b], cfg, _now=1010.0)
    assert a in result.suppressed
    assert b in result.allowed


def test_state_persisted_to_file(state_file: str) -> None:
    cfg = DebounceConfig(cooldown_seconds=60, state_path=state_file)
    status = _make_status("pipe_x", AlertLevel.WARNING)
    debounce([status], cfg, _now=2000.0)
    data = json.loads(Path(state_file).read_text())
    key = _entry_key(status)
    assert key in data
    assert data[key]["last_fired"] == 2000.0


def test_fire_count_increments_after_cooldown(state_file: str) -> None:
    cfg = DebounceConfig(cooldown_seconds=10, state_path=state_file)
    status = _make_status("pipe_y", AlertLevel.CRITICAL)
    debounce([status], cfg, _now=100.0)
    debounce([status], cfg, _now=120.0)  # past cooldown
    data = json.loads(Path(state_file).read_text())
    key = _entry_key(status)
    assert data[key]["fire_count"] == 2


def test_result_summary_string(state_file: str) -> None:
    cfg = DebounceConfig(cooldown_seconds=60, state_path=state_file)
    a = _make_status("pipe_a", AlertLevel.WARNING)
    b = _make_status("pipe_b", AlertLevel.CRITICAL)
    debounce([a], cfg, _now=1000.0)
    result = debounce([a, b], cfg, _now=1010.0)
    summary = result.summary()
    assert "allowed" in summary
    assert "suppressed" in summary
