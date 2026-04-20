"""Tests for pipewatch.projector."""
from __future__ import annotations

import pytest

from pipewatch.history import RunRecord
from pipewatch.projector import (
    ProjectionPoint,
    ProjectionResult,
    _linear_extrapolate,
    project,
    project_all,
)


def _make_record(pipeline: str, error_rate: float, latency_ms: float, ts: str = "2024-01-01T00:00:00") -> RunRecord:
    return RunRecord(
        pipeline=pipeline,
        timestamp=ts,
        level="ok",
        error_rate=error_rate,
        latency_ms=latency_ms,
        rows_processed=1000,
    )


_stable = [
    _make_record("pipe_a", 0.01, 120.0, "2024-01-01T00:00:00"),
    _make_record("pipe_a", 0.01, 120.0, "2024-01-01T01:00:00"),
    _make_record("pipe_a", 0.01, 120.0, "2024-01-01T02:00:00"),
]

_rising = [
    _make_record("pipe_b", 0.01, 100.0, "2024-01-01T00:00:00"),
    _make_record("pipe_b", 0.02, 110.0, "2024-01-01T01:00:00"),
    _make_record("pipe_b", 0.03, 120.0, "2024-01-01T02:00:00"),
]


def test_linear_extrapolate_stable():
    result = _linear_extrapolate([0.01, 0.01, 0.01], 3)
    assert result == [pytest.approx(0.01)] * 3


def test_linear_extrapolate_rising():
    result = _linear_extrapolate([0.01, 0.02], 2)
    assert result[0] == pytest.approx(0.03)
    assert result[1] == pytest.approx(0.04)


def test_linear_extrapolate_clamped_at_zero():
    result = _linear_extrapolate([0.02, 0.01], 3)
    assert all(v >= 0.0 for v in result)


def test_linear_extrapolate_single_value():
    result = _linear_extrapolate([0.05], 2)
    assert result == [pytest.approx(0.05), pytest.approx(0.05)]


def test_project_returns_none_for_empty_records():
    assert project("pipe_a", []) is None


def test_project_returns_none_for_single_record():
    assert project("pipe_a", [_make_record("pipe_a", 0.01, 100.0)]) is None


def test_project_returns_projection_result():
    result = project("pipe_a", _stable, steps=3)
    assert isinstance(result, ProjectionResult)


def test_project_correct_pipeline_name():
    result = project("pipe_a", _stable, steps=2)
    assert result.pipeline == "pipe_a"


def test_project_correct_step_count():
    result = project("pipe_a", _stable, steps=4)
    assert result.steps == 4
    assert len(result.points) == 4


def test_project_points_are_projection_point_instances():
    result = project("pipe_a", _stable, steps=2)
    assert all(isinstance(p, ProjectionPoint) for p in result.points)


def test_project_stable_error_rate_stays_stable():
    result = project("pipe_a", _stable, steps=3)
    for pt in result.points:
        assert pt.projected_error_rate == pytest.approx(0.01)


def test_project_rising_error_rate_increases():
    result = project("pipe_b", _rising, steps=2)
    assert result.points[0].projected_error_rate > _rising[-1].error_rate


def test_project_summary_contains_pipeline():
    result = project("pipe_a", _stable)
    assert "pipe_a" in result.summary()


def test_project_all_returns_list():
    records = _stable + _rising
    results = project_all(records, steps=2)
    assert isinstance(results, list)
    assert len(results) == 2


def test_project_all_skips_pipelines_with_one_record():
    single = [_make_record("lonely", 0.01, 100.0)]
    results = project_all(single, steps=2)
    assert results == []


def test_projection_point_summary_contains_step():
    pt = ProjectionPoint(step=2, projected_error_rate=0.05, projected_latency_ms=200.0)
    assert "step=2" in pt.summary()
