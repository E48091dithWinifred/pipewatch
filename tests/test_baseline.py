"""Tests for pipewatch.baseline."""
import json
import os
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.baseline import (
    BaselineDrift,
    capture_baseline,
    load_baseline,
    compare_to_baseline,
)


def _make_status(name: str, error_rate: float = 0.0, latency_ms: float = 100.0) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=AlertLevel.OK,
        error_rate=error_rate,
        latency_ms=latency_ms,
        message="ok",
    )


@pytest.fixture()
def tmp_dir(tmp_path):
    return str(tmp_path)


def test_capture_baseline_creates_file(tmp_dir):
    statuses = [_make_status("pipe_a", 0.02, 150.0)]
    capture_baseline(statuses, tmp_dir)
    assert os.path.exists(os.path.join(tmp_dir, "baseline_default.json"))


def test_capture_baseline_returns_entries(tmp_dir):
    statuses = [_make_status("pipe_a", 0.02, 150.0)]
    entries = capture_baseline(statuses, tmp_dir)
    assert "pipe_a" in entries
    assert entries["pipe_a"].avg_error_rate == pytest.approx(0.02)
    assert entries["pipe_a"].avg_latency_ms == pytest.approx(150.0)


def test_load_baseline_returns_none_when_missing(tmp_dir):
    result = load_baseline(tmp_dir, tag="nonexistent")
    assert result is None


def test_load_baseline_roundtrip(tmp_dir):
    statuses = [_make_status("pipe_b", 0.05, 200.0)]
    capture_baseline(statuses, tmp_dir, tag="v1")
    loaded = load_baseline(tmp_dir, tag="v1")
    assert loaded is not None
    assert "pipe_b" in loaded
    assert loaded["pipe_b"].avg_error_rate == pytest.approx(0.05)


def test_compare_no_drift():
    statuses = [_make_status("pipe_a", 0.02, 100.0)]
    baseline = {"pipe_a": __import__("pipewatch.baseline", fromlist=["BaselineEntry"]).BaselineEntry(
        pipeline_name="pipe_a", avg_error_rate=0.02, avg_latency_ms=100.0, sample_count=1
    )}
    report = compare_to_baseline(statuses, baseline)
    assert len(report.drifts) == 1
    assert not report.drifts[0].has_drift


def test_compare_detects_error_rate_drift():
    from pipewatch.baseline import BaselineEntry
    statuses = [_make_status("pipe_a", 0.10, 100.0)]
    baseline = {"pipe_a": BaselineEntry("pipe_a", 0.02, 100.0, 1)}
    report = compare_to_baseline(statuses, baseline)
    assert report.drifts[0].error_rate_delta == pytest.approx(0.08)
    assert report.drifts[0].has_drift


def test_compare_detects_latency_drift():
    from pipewatch.baseline import BaselineEntry
    statuses = [_make_status("pipe_a", 0.02, 300.0)]
    baseline = {"pipe_a": BaselineEntry("pipe_a", 0.02, 100.0, 1)}
    report = compare_to_baseline(statuses, baseline)
    assert report.drifts[0].latency_delta_ms == pytest.approx(200.0)


def test_compare_skips_unknown_pipeline():
    from pipewatch.baseline import BaselineEntry
    statuses = [_make_status("new_pipe", 0.05, 100.0)]
    baseline = {"old_pipe": BaselineEntry("old_pipe", 0.01, 50.0, 1)}
    report = compare_to_baseline(statuses, baseline)
    assert report.drifts == []


def test_degraded_filters_improvements():
    from pipewatch.baseline import BaselineEntry, BaselineDrift, BaselineReport
    report = BaselineReport(drifts=[
        BaselineDrift("pipe_a", error_rate_delta=0.05, latency_delta_ms=10.0),
        BaselineDrift("pipe_b", error_rate_delta=-0.02, latency_delta_ms=-5.0),
    ])
    assert len(report.degraded) == 1
    assert report.degraded[0].pipeline_name == "pipe_a"
