"""Tests for pipewatch.limiter."""
import pytest
from pipewatch.limiter import LimiterConfig, LimiterState, LimitResult, check_limit


def test_limiter_config_defaults():
    cfg = LimiterConfig()
    assert cfg.max_per_window == 5
    assert cfg.window_seconds == 60


def test_limiter_config_invalid_max_raises():
    with pytest.raises(ValueError):
        LimiterConfig(max_per_window=0)


def test_limiter_config_invalid_window_raises():
    with pytest.raises(ValueError):
        LimiterConfig(window_seconds=0)


def test_first_call_is_allowed():
    cfg = LimiterConfig(max_per_window=3, window_seconds=60)
    state = LimiterState()
    result = check_limit("pipeline_a", state, cfg, now=1000.0)
    assert result.allowed is True


def test_within_limit_all_allowed():
    cfg = LimiterConfig(max_per_window=3, window_seconds=60)
    state = LimiterState()
    for i in range(3):
        result = check_limit("pipeline_a", state, cfg, now=1000.0 + i)
    assert result.allowed is True


def test_exceeds_limit_is_blocked():
    cfg = LimiterConfig(max_per_window=3, window_seconds=60)
    state = LimiterState()
    for _ in range(3):
        check_limit("pipeline_a", state, cfg, now=1000.0)
    result = check_limit("pipeline_a", state, cfg, now=1001.0)
    assert result.allowed is False


def test_window_expiry_allows_again():
    cfg = LimiterConfig(max_per_window=2, window_seconds=30)
    state = LimiterState()
    check_limit("pipe", state, cfg, now=1000.0)
    check_limit("pipe", state, cfg, now=1001.0)
    # advance past window
    result = check_limit("pipe", state, cfg, now=1035.0)
    assert result.allowed is True


def test_different_keys_are_independent():
    cfg = LimiterConfig(max_per_window=1, window_seconds=60)
    state = LimiterState()
    check_limit("pipe_a", state, cfg, now=1000.0)
    result = check_limit("pipe_b", state, cfg, now=1000.0)
    assert result.allowed is True


def test_result_count_in_window():
    cfg = LimiterConfig(max_per_window=5, window_seconds=60)
    state = LimiterState()
    check_limit("pipe", state, cfg, now=1000.0)
    check_limit("pipe", state, cfg, now=1001.0)
    result = check_limit("pipe", state, cfg, now=1002.0)
    assert result.count_in_window == 3


def test_blocked_result_does_not_increment_count():
    cfg = LimiterConfig(max_per_window=2, window_seconds=60)
    state = LimiterState()
    check_limit("pipe", state, cfg, now=1000.0)
    check_limit("pipe", state, cfg, now=1001.0)
    result = check_limit("pipe", state, cfg, now=1002.0)
    assert result.allowed is False
    assert result.count_in_window == 2


def test_summary_allowed():
    r = LimitResult(key="pipe", allowed=True, count_in_window=1, max_per_window=5)
    assert "allowed" in r.summary()
    assert "pipe" in r.summary()


def test_summary_blocked():
    r = LimitResult(key="pipe", allowed=False, count_in_window=5, max_per_window=5)
    assert "blocked" in r.summary()
