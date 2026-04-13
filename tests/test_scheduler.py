"""Tests for pipewatch.scheduler."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.scheduler import SchedulerConfig, SchedulerStats, run_scheduler


# ---------------------------------------------------------------------------
# SchedulerStats unit tests
# ---------------------------------------------------------------------------

def test_stats_initial_totals():
    stats = SchedulerStats()
    assert stats.total_runs == 0
    assert stats.runs_completed == 0
    assert stats.runs_failed == 0


def test_stats_record_success():
    stats = SchedulerStats()
    stats.record_success()
    assert stats.runs_completed == 1
    assert stats.total_runs == 1
    assert stats.last_run_at is not None


def test_stats_record_failure():
    stats = SchedulerStats()
    stats.record_failure("boom")
    assert stats.runs_failed == 1
    assert stats.total_runs == 1
    assert "boom" in stats.errors


# ---------------------------------------------------------------------------
# run_scheduler integration tests (time patched)
# ---------------------------------------------------------------------------

@pytest.fixture()
def no_sleep():
    """Patch time.sleep to avoid real delays."""
    with patch("pipewatch.scheduler.time.sleep") as mock_sleep:
        yield mock_sleep


def test_run_scheduler_max_runs(no_sleep):
    task = MagicMock()
    cfg = SchedulerConfig(interval_seconds=1, max_runs=3)
    stats = run_scheduler(task, cfg)
    assert task.call_count == 3
    assert stats.runs_completed == 3
    assert stats.runs_failed == 0


def test_run_scheduler_counts_failures(no_sleep):
    task = MagicMock(side_effect=RuntimeError("fail"))
    cfg = SchedulerConfig(interval_seconds=1, max_runs=2)
    stats = run_scheduler(task, cfg)
    assert stats.runs_failed == 2
    assert stats.runs_completed == 0
    assert len(stats.errors) == 2


def test_run_scheduler_mixed_results(no_sleep):
    results = [None, RuntimeError("oops"), None]

    def task():
        val = results.pop(0)
        if isinstance(val, Exception):
            raise val

    cfg = SchedulerConfig(interval_seconds=1, max_runs=3)
    stats = run_scheduler(task, cfg)
    assert stats.runs_completed == 2
    assert stats.runs_failed == 1


def test_run_scheduler_stop_event(no_sleep):
    stop = threading.Event()
    call_count = 0

    def task():
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            stop.set()

    cfg = SchedulerConfig(interval_seconds=1)
    stats = run_scheduler(task, cfg, stop_event=stop)
    assert stats.runs_completed == 2


def test_run_scheduler_sleeps_between_runs(no_sleep):
    task = MagicMock()
    cfg = SchedulerConfig(interval_seconds=30, max_runs=2)
    run_scheduler(task, cfg)
    # Sleep called once (between run 1 and run 2; not after final run)
    assert no_sleep.call_count == 1
    no_sleep.assert_called_with(30)
