"""Tests for pipewatch.trend module."""
from __future__ import annotations

from typing import List

import pytest

from pipewatch.history import RunRecord
from pipewatch.trend import TrendReport, analyze_trend, _is_degrading, _is_improving


def _make_record(name: str, error_rate: float, latency_ms: float) -> RunRecord:
    return RunRecord(
        pipeline_name=name,
        timestamp="2024-01-01T00:00:00",
        alert_level="OK",
        error_rate=error_rate,
        latency_ms=latency_ms,
        rows_processed=1000,
    )


def _stable_records(n: int = 9) -> List[RunRecord]:
    return [_make_record("pipe_a", 0.01, 120.0) for _ in range(n)]


def test_analyze_trend_returns_none_for_empty():
    assert analyze_trend([], "pipe_a") is None


def test_analyze_trend_returns_none_for_unknown_pipeline():
    records = _stable_records()
    assert analyze_trend(records, "nonexistent") is None


def test_analyze_trend_returns_trend_report():
    records = _stable_records()
    result = analyze_trend(records, "pipe_a")
    assert isinstance(result, TrendReport)
    assert result.pipeline_name == "pipe_a"
    assert result.sample_count == 9


def test_analyze_trend_avg_values():
    records = _stable_records()
    result = analyze_trend(records, "pipe_a")
    assert abs(result.avg_error_rate - 0.01) < 1e-9
    assert abs(result.avg_latency_ms - 120.0) < 1e-9


def test_analyze_trend_respects_window():
    records = [_make_record("pipe_a", 0.05, 200.0)] * 5 + _stable_records(15)
    result = analyze_trend(records, "pipe_a", window=10)
    assert result.sample_count == 10
    assert abs(result.avg_error_rate - 0.01) < 1e-9


def test_analyze_trend_degrading():
    low = [_make_record("p", 0.01, 100.0)] * 5
    high = [_make_record("p", 0.10, 500.0)] * 5
    result = analyze_trend(low + high, "p")
    assert result.degrading is True
    assert result.improving is False


def test_analyze_trend_improving():
    high = [_make_record("p", 0.10, 500.0)] * 5
    low = [_make_record("p", 0.01, 100.0)] * 5
    result = analyze_trend(high + low, "p")
    assert result.improving is True
    assert result.degrading is False


def test_is_degrading_short_list():
    assert _is_degrading([0.1, 0.2]) is False


def test_is_improving_short_list():
    assert _is_improving([0.5]) is False


def test_summary_contains_pipeline_name():
    records = _stable_records()
    result = analyze_trend(records, "pipe_a")
    assert "pipe_a" in result.summary()
    assert "stable" in result.summary()
