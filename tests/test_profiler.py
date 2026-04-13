"""Tests for pipewatch.profiler."""
import time

import pytest

from pipewatch.profiler import ProfileEntry, ProfilerSession


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(session: ProfilerSession, name: str, *, records: int = 100, errors: int = 0, sleep: float = 0.0) -> ProfileEntry:
    session.start(name)
    if sleep:
        time.sleep(sleep)
    entry = session.stop(name, records_processed=records, error_count=errors)
    assert entry is not None
    return entry


# ---------------------------------------------------------------------------
# ProfileEntry
# ---------------------------------------------------------------------------

def test_throughput_normal():
    entry = ProfileEntry(
        pipeline_name="p",
        started_at=0.0,
        ended_at=1.0,
        duration_ms=1000.0,
        records_processed=500,
        error_count=0,
    )
    assert entry.throughput == pytest.approx(500.0)


def test_throughput_zero_duration():
    entry = ProfileEntry(
        pipeline_name="p",
        started_at=0.0,
        ended_at=0.0,
        duration_ms=0.0,
        records_processed=100,
        error_count=0,
    )
    assert entry.throughput == 0.0


# ---------------------------------------------------------------------------
# ProfilerSession
# ---------------------------------------------------------------------------

def test_stop_without_start_returns_none():
    session = ProfilerSession()
    result = session.stop("ghost", records_processed=10)
    assert result is None


def test_entry_recorded_after_stop():
    session = ProfilerSession()
    entry = _run(session, "etl")
    assert len(session.entries) == 1
    assert session.entries[0] is entry


def test_duration_is_positive():
    session = ProfilerSession()
    entry = _run(session, "etl", sleep=0.01)
    assert entry.duration_ms > 0


def test_get_entries_filters_by_name():
    session = ProfilerSession()
    _run(session, "alpha")
    _run(session, "beta")
    _run(session, "alpha")
    assert len(session.get_entries("alpha")) == 2
    assert len(session.get_entries("beta")) == 1


def test_average_duration_ms_returns_none_for_unknown():
    session = ProfilerSession()
    assert session.average_duration_ms("nope") is None


def test_average_duration_ms_single_run():
    session = ProfilerSession()
    entry = _run(session, "pipe")
    avg = session.average_duration_ms("pipe")
    assert avg == pytest.approx(entry.duration_ms)


def test_slowest_returns_none_for_empty_session():
    session = ProfilerSession()
    assert session.slowest() is None


def test_slowest_returns_longest_entry():
    session = ProfilerSession()
    _run(session, "fast")
    slow_entry = _run(session, "slow", sleep=0.02)
    assert session.slowest() is slow_entry
