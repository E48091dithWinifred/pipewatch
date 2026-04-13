"""Tests for pipewatch.grouper."""

from __future__ import annotations

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.grouper import (
    PipelineGroup,
    group_by,
    group_by_level,
    group_by_prefix,
)


def _make_status(
    name: str,
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.0,
) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        error_rate=error_rate,
        latency_ms=100.0,
        message="",
    )


@pytest.fixture()
def sample_statuses():
    return [
        _make_status("etl_sales", AlertLevel.OK, 0.01),
        _make_status("etl_orders", AlertLevel.WARNING, 0.05),
        _make_status("ml_forecast", AlertLevel.CRITICAL, 0.15),
        _make_status("ml_churn", AlertLevel.OK, 0.02),
        _make_status("etl_returns", AlertLevel.CRITICAL, 0.20),
    ]


def test_group_by_level_keys(sample_statuses):
    groups = group_by_level(sample_statuses)
    assert set(groups.keys()) == {"ok", "warning", "critical"}


def test_group_by_level_counts(sample_statuses):
    groups = group_by_level(sample_statuses)
    assert groups["ok"].count == 2
    assert groups["warning"].count == 1
    assert groups["critical"].count == 2


def test_group_by_prefix_keys(sample_statuses):
    groups = group_by_prefix(sample_statuses)
    assert set(groups.keys()) == {"etl", "ml"}


def test_group_by_prefix_counts(sample_statuses):
    groups = group_by_prefix(sample_statuses)
    assert groups["etl"].count == 3
    assert groups["ml"].count == 2


def test_pipeline_group_worst_level_critical(sample_statuses):
    groups = group_by_prefix(sample_statuses)
    assert groups["etl"].worst_level == AlertLevel.CRITICAL


def test_pipeline_group_worst_level_ok():
    statuses = [_make_status("etl_a"), _make_status("etl_b")]
    groups = group_by_prefix(statuses)
    assert groups["etl"].worst_level == AlertLevel.OK


def test_pipeline_group_avg_error_rate(sample_statuses):
    groups = group_by_prefix(sample_statuses)
    etl_group = groups["etl"]
    expected = (0.01 + 0.05 + 0.20) / 3
    assert abs(etl_group.avg_error_rate - expected) < 1e-9


def test_pipeline_group_level_counts(sample_statuses):
    groups = group_by_prefix(sample_statuses)
    etl = groups["etl"]
    assert etl.ok_count == 1
    assert etl.warning_count == 1
    assert etl.critical_count == 1


def test_group_by_skips_none_keys():
    statuses = [_make_status("noseparator")]
    # key_fn returns None for all — result should be empty
    groups = group_by(statuses, lambda _s: None)
    assert groups == {}


def test_group_by_empty_input():
    groups = group_by_level([])
    assert groups == {}


def test_pipeline_group_avg_error_rate_empty():
    pg = PipelineGroup(key="test")
    assert pg.avg_error_rate == 0.0
