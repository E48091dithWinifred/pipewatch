"""Tests for pipewatch.watcher module."""
import pytest
from unittest.mock import MagicMock

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.watcher import WatchEvent, WatcherConfig, process_pipeline_status, watch_once


def _make_status(name="orders", level=AlertLevel.OK, error_rate=0.01,
                 latency_ms=100.0, rows_per_second=500.0, message="") -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message=message,
        error_rate=error_rate,
        latency_ms=latency_ms,
        rows_per_second=rows_per_second,
    )


def test_first_run_has_no_diff(tmp_path):
    cfg = WatcherConfig(snapshot_dir=str(tmp_path))
    status = _make_status()
    event = process_pipeline_status(status, cfg)
    assert event.is_first_run is True
    assert event.diff is None


def test_second_run_has_diff(tmp_path):
    cfg = WatcherConfig(snapshot_dir=str(tmp_path))
    status1 = _make_status(error_rate=0.01)
    process_pipeline_status(status1, cfg)

    status2 = _make_status(error_rate=0.05)
    event = process_pipeline_status(status2, cfg)
    assert event.is_first_run is False
    assert event.diff is not None
    assert event.diff.error_rate_delta == pytest.approx(0.04, abs=1e-6)


def test_on_event_callback_called(tmp_path):
    callback = MagicMock()
    cfg = WatcherConfig(snapshot_dir=str(tmp_path), on_event=callback)
    status = _make_status()
    process_pipeline_status(status, cfg)
    callback.assert_called_once()
    args = callback.call_args[0]
    assert isinstance(args[0], WatchEvent)


def test_watch_once_returns_all_events(tmp_path):
    cfg = WatcherConfig(snapshot_dir=str(tmp_path))
    statuses = [_make_status(name="a"), _make_status(name="b"), _make_status(name="c")]
    events = watch_once(statuses, cfg)
    assert len(events) == 3
    names = {e.pipeline_name for e in events}
    assert names == {"a", "b", "c"}


def test_level_change_reflected_in_diff(tmp_path):
    cfg = WatcherConfig(snapshot_dir=str(tmp_path))
    process_pipeline_status(_make_status(level=AlertLevel.OK), cfg)
    event = process_pipeline_status(_make_status(level=AlertLevel.CRITICAL), cfg)
    assert event.diff.level_changed is True
    assert event.diff.previous_level == "OK"
    assert event.diff.current_level == "CRITICAL"


def test_watch_event_stores_status(tmp_path):
    cfg = WatcherConfig(snapshot_dir=str(tmp_path))
    status = _make_status(name="payments", level=AlertLevel.WARNING)
    event = process_pipeline_status(status, cfg)
    assert event.status is status
    assert event.pipeline_name == "payments"
