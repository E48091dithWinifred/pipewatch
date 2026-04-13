"""Tests for pipewatch.scorer."""
from __future__ import annotations

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.scorer import (
    PipelineScore,
    _error_rate_component,
    _latency_component,
    _level_component,
    score_all,
    score_pipeline,
)


def _make_status(
    name: str = "pipe",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.0,
    latency_ms: float = 0.0,
    message: str = "",
) -> PipelineStatus:
    return PipelineStatus(
        pipeline=name, level=level, error_rate=error_rate,
        latency_ms=latency_ms, message=message,
    )


# --- component helpers ---

def test_level_component_ok():
    assert _level_component(AlertLevel.OK) == 100.0


def test_level_component_warning():
    assert _level_component(AlertLevel.WARNING) == 50.0


def test_level_component_critical():
    assert _level_component(AlertLevel.CRITICAL) == 0.0


def test_error_rate_component_zero():
    assert _error_rate_component(0.0) == 100.0


def test_error_rate_component_full():
    assert _error_rate_component(1.0) == 0.0


def test_error_rate_component_half():
    assert _error_rate_component(0.5) == pytest.approx(50.0)


def test_latency_component_zero():
    assert _latency_component(0.0) == 100.0


def test_latency_component_at_ceiling():
    assert _latency_component(5000.0) == 0.0


def test_latency_component_clamped_below_zero():
    assert _latency_component(9999.0) == 0.0


# --- score_pipeline ---

def test_score_pipeline_perfect():
    ps = score_pipeline(_make_status(level=AlertLevel.OK, error_rate=0.0, latency_ms=0.0))
    assert ps.score == pytest.approx(100.0)
    assert ps.grade == "A"


def test_score_pipeline_worst():
    ps = score_pipeline(
        _make_status(level=AlertLevel.CRITICAL, error_rate=1.0, latency_ms=5000.0)
    )
    assert ps.score == pytest.approx(0.0)
    assert ps.grade == "F"


def test_score_pipeline_returns_pipeline_score_type():
    ps = score_pipeline(_make_status())
    assert isinstance(ps, PipelineScore)


def test_score_pipeline_grade_b():
    # WARNING level (50), low error rate, low latency → should be B range
    ps = score_pipeline(_make_status(level=AlertLevel.WARNING, error_rate=0.02, latency_ms=100.0))
    assert ps.grade in ("A", "B")


# --- score_all ---

def test_score_all_sorted_descending():
    statuses = [
        _make_status("bad", AlertLevel.CRITICAL, error_rate=0.5, latency_ms=3000.0),
        _make_status("good", AlertLevel.OK, error_rate=0.0, latency_ms=0.0),
        _make_status("mid", AlertLevel.WARNING, error_rate=0.1, latency_ms=500.0),
    ]
    scores = score_all(statuses)
    assert scores[0].pipeline == "good"
    assert scores[-1].pipeline == "bad"


def test_score_all_empty():
    assert score_all([]) == []


def test_score_all_length_matches_input():
    statuses = [_make_status(str(i)) for i in range(5)]
    assert len(score_all(statuses)) == 5
