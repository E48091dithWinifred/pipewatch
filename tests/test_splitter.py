"""Tests for pipewatch.splitter."""
from __future__ import annotations

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.splitter import (
    SplitResult,
    split_by_level,
    split_by_predicate,
    split_by_prefix,
)


def _make_status(name: str, level: AlertLevel, error_rate: float = 0.0) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message="ok",
        error_rate=error_rate,
        latency_ms=100.0,
    )


@pytest.fixture()
def sample_statuses():
    return [
        _make_status("ingest_orders", AlertLevel.OK),
        _make_status("ingest_users", AlertLevel.OK),
        _make_status("transform_orders", AlertLevel.WARNING, 0.05),
        _make_status("transform_users", AlertLevel.CRITICAL, 0.20),
        _make_status("export_reports", AlertLevel.WARNING, 0.07),
    ]


def test_split_by_level_keys(sample_statuses):
    result = split_by_level(sample_statuses)
    assert set(result.partition_names) == {"ok", "warning", "critical"}


def test_split_by_level_counts(sample_statuses):
    result = split_by_level(sample_statuses)
    assert len(result.get("ok")) == 2
    assert len(result.get("warning")) == 2
    assert len(result.get("critical")) == 1


def test_split_by_level_total_count(sample_statuses):
    result = split_by_level(sample_statuses)
    assert result.total_count == len(sample_statuses)


def test_split_by_level_empty_returns_empty_result():
    result = split_by_level([])
    assert result.total_count == 0
    assert result.partition_names == []


def test_split_by_prefix_keys(sample_statuses):
    result = split_by_prefix(sample_statuses)
    assert set(result.partition_names) == {"ingest", "transform", "export"}


def test_split_by_prefix_counts(sample_statuses):
    result = split_by_prefix(sample_statuses)
    assert len(result.get("ingest")) == 2
    assert len(result.get("transform")) == 2
    assert len(result.get("export")) == 1


def test_split_by_prefix_missing_key_returns_empty(sample_statuses):
    result = split_by_prefix(sample_statuses)
    assert result.get("nonexistent") == []


def test_split_by_predicate_custom_key(sample_statuses):
    result = split_by_predicate(
        sample_statuses,
        predicate=lambda s: "high" if s.error_rate >= 0.1 else "low",
    )
    assert len(result.get("high")) == 1
    assert len(result.get("low")) == 4


def test_split_result_summary_contains_keys(sample_statuses):
    result = split_by_level(sample_statuses)
    s = result.summary()
    assert "ok" in s
    assert "warning" in s
    assert "critical" in s


def test_split_result_summary_format(sample_statuses):
    result = split_by_level(sample_statuses)
    assert result.summary().startswith("SplitResult(")
