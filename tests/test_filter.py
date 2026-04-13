"""Tests for pipewatch.filter."""
from __future__ import annotations

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.filter import FilterConfig, apply_filter


def _make_status(
    name: str,
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.0,
) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message="",
        error_rate=error_rate,
        latency_ms=100.0,
    )


@pytest.fixture()
def sample_statuses():
    return [
        _make_status("ingest_orders", AlertLevel.OK, 0.01),
        _make_status("transform_users", AlertLevel.WARNING, 0.05),
        _make_status("export_reports", AlertLevel.CRITICAL, 0.15),
        _make_status("ingest_payments", AlertLevel.OK, 0.0),
    ]


def test_empty_filter_returns_all(sample_statuses):
    cfg = FilterConfig()
    result = apply_filter(sample_statuses, cfg)
    assert len(result) == 4


def test_filter_by_single_level(sample_statuses):
    cfg = FilterConfig(levels=["WARNING"])
    result = apply_filter(sample_statuses, cfg)
    assert len(result) == 1
    assert result[0].pipeline_name == "transform_users"


def test_filter_by_multiple_levels(sample_statuses):
    cfg = FilterConfig(levels=["WARNING", "CRITICAL"])
    result = apply_filter(sample_statuses, cfg)
    names = {s.pipeline_name for s in result}
    assert names == {"transform_users", "export_reports"}


def test_filter_by_name_contains(sample_statuses):
    cfg = FilterConfig(name_contains="ingest")
    result = apply_filter(sample_statuses, cfg)
    assert len(result) == 2
    assert all("ingest" in s.pipeline_name for s in result)


def test_filter_by_name_contains_case_insensitive(sample_statuses):
    cfg = FilterConfig(name_contains="INGEST")
    result = apply_filter(sample_statuses, cfg)
    assert len(result) == 2


def test_filter_by_max_error_rate(sample_statuses):
    cfg = FilterConfig(max_error_rate=0.05)
    result = apply_filter(sample_statuses, cfg)
    assert all(s.error_rate <= 0.05 for s in result)
    assert len(result) == 3


def test_filter_by_min_error_rate(sample_statuses):
    cfg = FilterConfig(min_error_rate=0.05)
    result = apply_filter(sample_statuses, cfg)
    assert all(s.error_rate >= 0.05 for s in result)
    assert len(result) == 2


def test_filter_combined_level_and_name(sample_statuses):
    cfg = FilterConfig(levels=["OK"], name_contains="ingest")
    result = apply_filter(sample_statuses, cfg)
    assert len(result) == 2
    assert all(s.level == AlertLevel.OK for s in result)


def test_filter_no_match_returns_empty(sample_statuses):
    cfg = FilterConfig(levels=["CRITICAL"], name_contains="ingest")
    result = apply_filter(sample_statuses, cfg)
    assert result == []
