"""Tests for pipewatch.inverter."""
from __future__ import annotations

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.filter import FilterConfig
from pipewatch.inverter import InvertResult, invert_filter


def _make_status(name: str, level: AlertLevel, error_rate: float = 0.0) -> PipelineStatus:
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
        _make_status("alpha", AlertLevel.OK, 0.01),
        _make_status("beta", AlertLevel.WARNING, 0.05),
        _make_status("gamma", AlertLevel.CRITICAL, 0.20),
    ]


def test_invert_result_summary():
    r = InvertResult(kept=[_make_status("a", AlertLevel.OK)], dropped=[])
    assert "kept=1" in r.summary()
    assert "dropped=0" in r.summary()


def test_invert_result_counts():
    s = _make_status("x", AlertLevel.WARNING)
    r = InvertResult(kept=[s], dropped=[s, s])
    assert r.kept_count == 1
    assert r.dropped_count == 2


def test_invert_empty_statuses():
    result = invert_filter([], FilterConfig(levels=[AlertLevel.CRITICAL]))
    assert result.kept == []
    assert result.dropped == []


def test_invert_by_level_keeps_non_matching(sample_statuses):
    cfg = FilterConfig(levels=[AlertLevel.CRITICAL])
    result = invert_filter(sample_statuses, cfg)
    names = [s.pipeline_name for s in result.kept]
    assert "alpha" in names
    assert "beta" in names
    assert "gamma" not in names


def test_invert_by_level_drops_matching(sample_statuses):
    cfg = FilterConfig(levels=[AlertLevel.CRITICAL])
    result = invert_filter(sample_statuses, cfg)
    assert any(s.pipeline_name == "gamma" for s in result.dropped)


def test_invert_no_filter_config_keeps_nothing(sample_statuses):
    """Empty FilterConfig passes everything, so invert keeps nothing."""
    cfg = FilterConfig()
    result = invert_filter(sample_statuses, cfg)
    assert result.kept_count == 0
    assert result.dropped_count == len(sample_statuses)


def test_invert_preserves_all_statuses(sample_statuses):
    cfg = FilterConfig(levels=[AlertLevel.WARNING])
    result = invert_filter(sample_statuses, cfg)
    total = result.kept_count + result.dropped_count
    assert total == len(sample_statuses)
