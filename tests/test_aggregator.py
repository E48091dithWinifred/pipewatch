"""Tests for pipewatch.aggregator."""
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.metrics import PipelineMetrics
from pipewatch.aggregator import aggregate, group_by_level, AggregatedStats


def _make_status(
    name: str,
    level: AlertLevel,
    error_rate: float = 0.0,
    latency_ms: float = 100.0,
) -> PipelineStatus:
    metrics = PipelineMetrics(
        pipeline_name=name,
        total_records=1000,
        failed_records=int(error_rate * 1000),
        duration_seconds=1.0,
        latency_p99_ms=latency_ms,
    )
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message="test",
        metrics=metrics,
    )


def test_aggregate_empty_returns_defaults():
    stats = aggregate([])
    assert stats.total == 0
    assert stats.health_ratio == 1.0


def test_aggregate_counts_levels():
    statuses = [
        _make_status("a", AlertLevel.OK),
        _make_status("b", AlertLevel.WARNING),
        _make_status("c", AlertLevel.CRITICAL),
        _make_status("d", AlertLevel.OK),
    ]
    stats = aggregate(statuses)
    assert stats.total == 4
    assert stats.ok_count == 2
    assert stats.warning_count == 1
    assert stats.critical_count == 1


def test_aggregate_health_ratio():
    statuses = [
        _make_status("a", AlertLevel.OK),
        _make_status("b", AlertLevel.OK),
        _make_status("c", AlertLevel.CRITICAL),
    ]
    stats = aggregate(statuses)
    assert abs(stats.health_ratio - 2 / 3) < 1e-9


def test_aggregate_avg_error_rate():
    statuses = [
        _make_status("a", AlertLevel.OK, error_rate=0.1),
        _make_status("b", AlertLevel.WARNING, error_rate=0.3),
    ]
    stats = aggregate(statuses)
    assert abs(stats.avg_error_rate - 0.2) < 1e-9


def test_aggregate_max_latency():
    statuses = [
        _make_status("a", AlertLevel.OK, latency_ms=50.0),
        _make_status("b", AlertLevel.OK, latency_ms=200.0),
    ]
    stats = aggregate(statuses)
    assert stats.max_latency_ms == 200.0


def test_aggregate_pipeline_names():
    statuses = [
        _make_status("pipe-alpha", AlertLevel.OK),
        _make_status("pipe-beta", AlertLevel.CRITICAL),
    ]
    stats = aggregate(statuses)
    assert "pipe-alpha" in stats.pipeline_names
    assert "pipe-beta" in stats.pipeline_names


def test_level_counts_dict():
    statuses = [_make_status("a", AlertLevel.WARNING)]
    stats = aggregate(statuses)
    assert stats.level_counts["warning"] == 1
    assert stats.level_counts["ok"] == 0


def test_group_by_level_separates_correctly():
    statuses = [
        _make_status("a", AlertLevel.OK),
        _make_status("b", AlertLevel.CRITICAL),
        _make_status("c", AlertLevel.WARNING),
        _make_status("d", AlertLevel.OK),
    ]
    groups = group_by_level(statuses)
    assert len(groups["ok"]) == 2
    assert len(groups["warning"]) == 1
    assert len(groups["critical"]) == 1


def test_group_by_level_empty_input():
    groups = group_by_level([])
    assert groups["ok"] == []
    assert groups["warning"] == []
    assert groups["critical"] == []
