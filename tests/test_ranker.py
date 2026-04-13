"""Tests for pipewatch.ranker."""
from __future__ import annotations

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.metrics import PipelineMetrics
from pipewatch.ranker import RankedPipeline, rank_pipelines, worst_pipeline


def _make_status(
    name: str,
    level: AlertLevel,
    error_rate_val: float = 0.0,
    latency: float = 100.0,
) -> PipelineStatus:
    metrics = PipelineMetrics(
        pipeline_name=name,
        total_records=1000,
        failed_records=int(error_rate_val * 1000),
        duration_seconds=1.0,
        latency_ms=latency,
    )
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message="test",
        metrics=metrics,
    )


@pytest.fixture
def sample_statuses():
    return [
        _make_status("pipe-ok", AlertLevel.OK, 0.01, 50.0),
        _make_status("pipe-warn", AlertLevel.WARNING, 0.08, 200.0),
        _make_status("pipe-crit", AlertLevel.CRITICAL, 0.25, 800.0),
    ]


def test_rank_pipelines_empty_returns_empty():
    assert rank_pipelines([]) == []


def test_rank_pipelines_returns_ranked_pipeline_instances(sample_statuses):
    result = rank_pipelines(sample_statuses)
    assert all(isinstance(r, RankedPipeline) for r in result)


def test_rank_pipelines_worst_is_rank_one(sample_statuses):
    result = rank_pipelines(sample_statuses)
    assert result[0].rank == 1
    assert result[0].name == "pipe-crit"


def test_rank_pipelines_ok_is_last(sample_statuses):
    result = rank_pipelines(sample_statuses)
    assert result[-1].name == "pipe-ok"


def test_rank_pipelines_ranks_are_sequential(sample_statuses):
    result = rank_pipelines(sample_statuses)
    ranks = [r.rank for r in result]
    assert ranks == list(range(1, len(result) + 1))


def test_rank_pipelines_top_n_limits_results(sample_statuses):
    result = rank_pipelines(sample_statuses, top_n=2)
    assert len(result) == 2


def test_rank_pipelines_top_n_returns_worst(sample_statuses):
    result = rank_pipelines(sample_statuses, top_n=1)
    assert result[0].name == "pipe-crit"


def test_ranked_pipeline_is_critical_flag(sample_statuses):
    result = rank_pipelines(sample_statuses)
    crit = next(r for r in result if r.name == "pipe-crit")
    assert crit.is_critical is True
    assert crit.is_healthy is False


def test_ranked_pipeline_is_healthy_flag(sample_statuses):
    result = rank_pipelines(sample_statuses)
    ok = next(r for r in result if r.name == "pipe-ok")
    assert ok.is_healthy is True
    assert ok.is_critical is False


def test_ranked_pipeline_has_grade(sample_statuses):
    result = rank_pipelines(sample_statuses)
    for r in result:
        assert r.grade in {"A", "B", "C", "D", "F"}


def test_worst_pipeline_returns_none_for_empty():
    assert worst_pipeline([]) is None


def test_worst_pipeline_returns_single_worst(sample_statuses):
    result = worst_pipeline(sample_statuses)
    assert result is not None
    assert result.rank == 1
    assert result.name == "pipe-crit"
