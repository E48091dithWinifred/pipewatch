"""Tests for pipewatch.windower."""
import pytest
from pipewatch.windower import WindowConfig, WindowStats, compute_window, compute_all_windows
from pipewatch.history import RunRecord


def _make_record(pipeline: str, level: str, error_rate: float, latency_ms: float) -> RunRecord:
    return RunRecord(
        pipeline=pipeline,
        level=level,
        error_rate=error_rate,
        latency_ms=latency_ms,
        timestamp="2024-01-01T00:00:00",
    )


@pytest.fixture
def records():
    return [
        _make_record("etl_a", "ok", 0.01, 100.0),
        _make_record("etl_a", "warning", 0.05, 200.0),
        _make_record("etl_a", "critical", 0.15, 400.0),
        _make_record("etl_b", "ok", 0.0, 50.0),
        _make_record("etl_b", "ok", 0.0, 60.0),
    ]


def test_window_config_default():
    cfg = WindowConfig()
    assert cfg.window_size == 10


def test_window_config_invalid_raises():
    with pytest.raises(ValueError):
        WindowConfig(window_size=0)


def test_compute_window_returns_none_for_unknown(records):
    result = compute_window("no_such", records)
    assert result is None


def test_compute_window_returns_stats(records):
    result = compute_window("etl_a", records)
    assert isinstance(result, WindowStats)
    assert result.pipeline == "etl_a"


def test_compute_window_correct_counts(records):
    result = compute_window("etl_a", records)
    assert result.ok_count == 1
    assert result.warning_count == 1
    assert result.critical_count == 1


def test_compute_window_avg_error_rate(records):
    result = compute_window("etl_a", records)
    expected = (0.01 + 0.05 + 0.15) / 3
    assert abs(result.avg_error_rate - expected) < 1e-9


def test_compute_window_avg_latency(records):
    result = compute_window("etl_a", records)
    expected = (100.0 + 200.0 + 400.0) / 3
    assert abs(result.avg_latency_ms - expected) < 1e-9


def test_compute_window_dominant_level_critical(records):
    result = compute_window("etl_a", records)
    assert result.dominant_level == "critical"


def test_compute_window_dominant_level_ok(records):
    result = compute_window("etl_b", records)
    assert result.dominant_level == "ok"


def test_compute_window_respects_window_size(records):
    cfg = WindowConfig(window_size=1)
    result = compute_window("etl_a", records, cfg)
    assert result.total_records == 1
    assert result.critical_count == 1


def test_compute_window_summary_contains_pipeline(records):
    result = compute_window("etl_a", records)
    assert "etl_a" in result.summary()


def test_compute_all_windows_returns_all_pipelines(records):
    results = compute_all_windows(records)
    names = [r.pipeline for r in results]
    assert "etl_a" in names
    assert "etl_b" in names


def test_compute_all_windows_empty_returns_empty():
    assert compute_all_windows([]) == []
