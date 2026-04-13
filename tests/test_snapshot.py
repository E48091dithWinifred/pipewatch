"""Tests for pipewatch.snapshot module."""
import os
import pytest

from pipewatch.snapshot import (
    MetricSnapshot,
    SnapshotDiff,
    diff_snapshots,
    load_snapshot,
    make_snapshot,
    save_snapshot,
)


def _snap(name="orders", error_rate=0.01, latency_ms=120.0,
          rows_per_second=500.0, alert_level="OK") -> MetricSnapshot:
    return make_snapshot(name, error_rate, latency_ms, rows_per_second, alert_level)


def test_make_snapshot_fields():
    s = _snap()
    assert s.pipeline_name == "orders"
    assert s.error_rate == 0.01
    assert s.latency_ms == 120.0
    assert s.rows_per_second == 500.0
    assert s.alert_level == "OK"
    assert s.captured_at  # non-empty timestamp


def test_save_and_load_roundtrip(tmp_path):
    snap = _snap(name="payments")
    save_snapshot(snap, str(tmp_path))
    loaded = load_snapshot("payments", str(tmp_path))
    assert loaded is not None
    assert loaded.pipeline_name == snap.pipeline_name
    assert loaded.error_rate == snap.error_rate
    assert loaded.latency_ms == snap.latency_ms
    assert loaded.alert_level == snap.alert_level


def test_load_snapshot_missing_returns_none(tmp_path):
    result = load_snapshot("nonexistent", str(tmp_path))
    assert result is None


def test_save_creates_directory(tmp_path):
    subdir = tmp_path / "snapshots" / "nested"
    snap = _snap(name="etl")
    save_snapshot(snap, str(subdir))
    assert subdir.exists()


def test_diff_snapshots_deltas():
    prev = _snap(error_rate=0.01, latency_ms=100.0, rows_per_second=400.0, alert_level="OK")
    curr = _snap(error_rate=0.05, latency_ms=150.0, rows_per_second=350.0, alert_level="WARNING")
    diff = diff_snapshots(prev, curr)
    assert diff.error_rate_delta == pytest.approx(0.04, abs=1e-6)
    assert diff.latency_delta_ms == pytest.approx(50.0)
    assert diff.rows_per_second_delta == pytest.approx(-50.0)
    assert diff.level_changed is True
    assert diff.previous_level == "OK"
    assert diff.current_level == "WARNING"


def test_diff_snapshots_no_change():
    snap = _snap()
    diff = diff_snapshots(snap, snap)
    assert diff.error_rate_delta == 0.0
    assert diff.latency_delta_ms == 0.0
    assert diff.level_changed is False


def test_snapshot_filename_safe_chars(tmp_path):
    snap = _snap(name="my pipeline/v2")
    save_snapshot(snap, str(tmp_path))
    files = list(tmp_path.iterdir())
    assert len(files) == 1
    assert " " not in files[0].name
    assert "/" not in files[0].name


def test_overwrite_snapshot(tmp_path):
    snap1 = _snap(error_rate=0.01)
    snap2 = _snap(error_rate=0.99)
    save_snapshot(snap1, str(tmp_path))
    save_snapshot(snap2, str(tmp_path))
    loaded = load_snapshot("orders", str(tmp_path))
    assert loaded.error_rate == 0.99
