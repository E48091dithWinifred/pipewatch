"""Tests for pipewatch.throttler."""
import json
import os
from datetime import datetime, timedelta

import pytest

from pipewatch.throttler import (
    ThrottleConfig,
    ThrottleEntry,
    _entry_key,
    is_throttled,
    record_fire,
    reset_throttle,
)


@pytest.fixture
def cfg(tmp_path):
    return ThrottleConfig(
        cooldown_minutes=30,
        state_path=str(tmp_path / "throttle_state.json"),
    )


def test_entry_key_format():
    assert _entry_key("my_pipe", "WARNING") == "my_pipe::WARNING"


def test_is_throttled_no_state_returns_false(cfg):
    assert is_throttled("pipe_a", "WARNING", cfg) is False


def test_record_fire_creates_entry(cfg):
    entry = record_fire("pipe_a", "WARNING", cfg)
    assert entry.pipeline == "pipe_a"
    assert entry.level == "WARNING"
    assert entry.fire_count == 1


def test_record_fire_increments_count(cfg):
    record_fire("pipe_a", "WARNING", cfg)
    entry = record_fire("pipe_a", "WARNING", cfg)
    assert entry.fire_count == 2


def test_is_throttled_after_fire_returns_true(cfg):
    record_fire("pipe_a", "WARNING", cfg)
    assert is_throttled("pipe_a", "WARNING", cfg) is True


def test_is_throttled_expired_returns_false(cfg, tmp_path):
    # Write a stale entry manually
    old_time = (datetime.utcnow() - timedelta(minutes=60)).isoformat()
    state = {"pipe_a::CRITICAL": {"pipeline": "pipe_a", "level": "CRITICAL",
                                   "last_fired": old_time, "fire_count": 1}}
    with open(cfg.state_path, "w") as f:
        json.dump(state, f)
    assert is_throttled("pipe_a", "CRITICAL", cfg) is False


def test_is_throttled_different_level_not_throttled(cfg):
    record_fire("pipe_a", "WARNING", cfg)
    assert is_throttled("pipe_a", "CRITICAL", cfg) is False


def test_reset_throttle_removes_entry(cfg):
    record_fire("pipe_a", "WARNING", cfg)
    removed = reset_throttle("pipe_a", "WARNING", cfg)
    assert removed is True
    assert is_throttled("pipe_a", "WARNING", cfg) is False


def test_reset_throttle_missing_returns_false(cfg):
    assert reset_throttle("pipe_z", "WARNING", cfg) is False


def test_state_persisted_to_disk(cfg):
    record_fire("pipe_b", "CRITICAL", cfg)
    assert os.path.exists(cfg.state_path)
    with open(cfg.state_path) as f:
        data = json.load(f)
    assert "pipe_b::CRITICAL" in data
    assert data["pipe_b::CRITICAL"]["fire_count"] == 1
