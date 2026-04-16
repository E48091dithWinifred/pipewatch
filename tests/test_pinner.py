"""Tests for pipewatch.pinner."""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.pinner import (
    PinConfig,
    PinEntry,
    filter_pinned,
    is_pinned,
    pin_pipeline,
    unpin_pipeline,
)


def _make_status(name: str, level: AlertLevel = AlertLevel.OK) -> PipelineStatus:
    return PipelineStatus(pipeline_name=name, level=level, message="", error_rate=0.0, latency_ms=10.0)


@pytest.fixture()
def cfg(tmp_path: Path) -> PinConfig:
    return PinConfig(state_path=str(tmp_path / "pins.json"))


def _future() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()


def _past() -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()


def test_pin_creates_file(cfg: PinConfig) -> None:
    pin_pipeline("etl_a", cfg)
    assert Path(cfg.state_path).exists()


def test_pin_returns_entry(cfg: PinConfig) -> None:
    entry = pin_pipeline("etl_a", cfg, reason="maintenance")
    assert isinstance(entry, PinEntry)
    assert entry.pipeline == "etl_a"
    assert entry.reason == "maintenance"


def test_pin_permanent_is_active(cfg: PinConfig) -> None:
    entry = pin_pipeline("etl_a", cfg)
    assert entry.is_active() is True


def test_pin_future_expiry_is_active(cfg: PinConfig) -> None:
    entry = pin_pipeline("etl_a", cfg, expires_at=_future())
    assert entry.is_active() is True


def test_pin_past_expiry_is_inactive(cfg: PinConfig) -> None:
    entry = PinEntry(pipeline="etl_a", pinned_at="", expires_at=_past())
    assert entry.is_active() is False


def test_is_pinned_true_when_active(cfg: PinConfig) -> None:
    pin_pipeline("etl_a", cfg)
    assert is_pinned(_make_status("etl_a"), cfg) is True


def test_is_pinned_false_when_not_pinned(cfg: PinConfig) -> None:
    assert is_pinned(_make_status("etl_b"), cfg) is False


def test_is_pinned_false_after_expiry(cfg: PinConfig, tmp_path: Path) -> None:
    pin_pipeline("etl_a", cfg, expires_at=_past())
    assert is_pinned(_make_status("etl_a"), cfg) is False


def test_unpin_removes_entry(cfg: PinConfig) -> None:
    pin_pipeline("etl_a", cfg)
    removed = unpin_pipeline("etl_a", cfg)
    assert removed is True
    assert is_pinned(_make_status("etl_a"), cfg) is False


def test_unpin_returns_false_when_not_found(cfg: PinConfig) -> None:
    assert unpin_pipeline("etl_z", cfg) is False


def test_filter_pinned_partitions(cfg: PinConfig) -> None:
    pin_pipeline("etl_a", cfg)
    statuses = [_make_status("etl_a"), _make_status("etl_b"), _make_status("etl_c")]
    unpinned, pinned = filter_pinned(statuses, cfg)
    assert len(pinned) == 1
    assert pinned[0].pipeline_name == "etl_a"
    assert len(unpinned) == 2


def test_pin_overwrites_existing(cfg: PinConfig) -> None:
    pin_pipeline("etl_a", cfg, reason="first")
    pin_pipeline("etl_a", cfg, reason="second")
    data = json.loads(Path(cfg.state_path).read_text())
    assert len(data) == 1
    assert data[0]["reason"] == "second"
