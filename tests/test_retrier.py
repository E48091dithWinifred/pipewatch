"""Tests for pipewatch.retrier."""
import json
import os
import pytest

from pipewatch.checker import AlertLevel
from pipewatch.retrier import (
    RetryPolicy,
    RetryEntry,
    record_attempt,
    resolve,
    get_pending,
    _load_state,
)


@pytest.fixture
def state_file(tmp_path):
    return str(tmp_path / "retry_state.json")


@pytest.fixture
def policy():
    return RetryPolicy(max_attempts=3, backoff_seconds=10.0, retry_on=["CRITICAL"])


def test_record_attempt_creates_entry(state_file):
    entry = record_attempt("pipe_a", AlertLevel.CRITICAL, state_file, now=1000.0)
    assert entry.pipeline == "pipe_a"
    assert entry.attempts == 1
    assert entry.level == "CRITICAL"
    assert not entry.resolved


def test_record_attempt_increments_on_second_call(state_file):
    record_attempt("pipe_a", AlertLevel.CRITICAL, state_file, now=1000.0)
    entry = record_attempt("pipe_a", AlertLevel.CRITICAL, state_file, now=1020.0)
    assert entry.attempts == 2


def test_record_attempt_persists_to_file(state_file):
    record_attempt("pipe_b", AlertLevel.CRITICAL, state_file, now=2000.0)
    assert os.path.exists(state_file)
    state = _load_state(state_file)
    assert "pipe_b" in state


def test_resolve_marks_entry(state_file):
    record_attempt("pipe_c", AlertLevel.CRITICAL, state_file, now=3000.0)
    entry = resolve("pipe_c", state_file)
    assert entry is not None
    assert entry.resolved is True


def test_resolve_missing_returns_none(state_file):
    result = resolve("nonexistent", state_file)
    assert result is None


def test_should_retry_true_when_eligible(policy):
    entry = RetryEntry("p", "CRITICAL", attempts=1, last_attempt_ts=0.0)
    assert entry.should_retry(policy, now=20.0) is True


def test_should_retry_false_when_resolved(policy):
    entry = RetryEntry("p", "CRITICAL", attempts=1, last_attempt_ts=0.0, resolved=True)
    assert entry.should_retry(policy, now=20.0) is False


def test_should_retry_false_when_max_attempts_reached(policy):
    entry = RetryEntry("p", "CRITICAL", attempts=3, last_attempt_ts=0.0)
    assert entry.should_retry(policy, now=20.0) is False


def test_should_retry_false_when_backoff_not_elapsed(policy):
    entry = RetryEntry("p", "CRITICAL", attempts=1, last_attempt_ts=100.0)
    assert entry.should_retry(policy, now=105.0) is False


def test_should_retry_false_wrong_level(policy):
    entry = RetryEntry("p", "WARNING", attempts=1, last_attempt_ts=0.0)
    assert entry.should_retry(policy, now=20.0) is False


def test_exhausted_true_when_max_reached(policy):
    entry = RetryEntry("p", "CRITICAL", attempts=3, last_attempt_ts=0.0)
    assert entry.exhausted(policy) is True


def test_exhausted_false_when_resolved(policy):
    entry = RetryEntry("p", "CRITICAL", attempts=3, last_attempt_ts=0.0, resolved=True)
    assert entry.exhausted(policy) is False


def test_get_pending_returns_eligible(state_file, policy):
    record_attempt("pipe_x", AlertLevel.CRITICAL, state_file, now=0.0)
    pending = get_pending(state_file, policy, now=20.0)
    assert any(e.pipeline == "pipe_x" for e in pending)


def test_get_pending_excludes_resolved(state_file, policy):
    record_attempt("pipe_y", AlertLevel.CRITICAL, state_file, now=0.0)
    resolve("pipe_y", state_file)
    pending = get_pending(state_file, policy, now=20.0)
    assert not any(e.pipeline == "pipe_y" for e in pending)
