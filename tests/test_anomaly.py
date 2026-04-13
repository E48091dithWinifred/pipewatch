"""Tests for pipewatch.anomaly."""
import pytest
from pipewatch.anomaly import (
    AnomalyResult,
    detect_anomaly,
    detect_all_anomalies,
    _mean,
    _std_dev,
    _z_score,
)
from pipewatch.history import RunRecord


def _make_record(name: str, error_rate: float, latency_ms: float) -> RunRecord:
    return RunRecord(
        pipeline_name=name,
        timestamp="2024-01-01T00:00:00",
        level="OK",
        error_rate=error_rate,
        latency_ms=latency_ms,
        rows_processed=1000,
    )


def _stable_records(name: str, n: int = 6) -> list:
    return [_make_record(name, 0.01, 100.0) for _ in range(n)]


# --- unit helpers ---

def test_mean_normal():
    assert _mean([1.0, 2.0, 3.0]) == pytest.approx(2.0)


def test_mean_empty_returns_zero():
    assert _mean([]) == 0.0


def test_std_dev_uniform_returns_zero():
    assert _std_dev([5.0, 5.0, 5.0], 5.0) == pytest.approx(0.0)


def test_std_dev_single_returns_zero():
    assert _std_dev([3.0], 3.0) == 0.0


def test_z_score_zero_std_returns_zero():
    assert _z_score(10.0, 5.0, 0.0) == 0.0


def test_z_score_normal():
    assert _z_score(7.0, 5.0, 2.0) == pytest.approx(1.0)


# --- detect_anomaly ---

def test_detect_anomaly_returns_none_for_too_few_records():
    records = [_make_record("pipe", 0.01, 100.0)] * 2
    result = detect_anomaly("pipe", records, "error_rate")
    assert result is None


def test_detect_anomaly_returns_none_for_unknown_pipeline():
    records = _stable_records("other_pipe")
    result = detect_anomaly("pipe", records, "error_rate")
    assert result is None


def test_detect_anomaly_no_anomaly_on_stable_data():
    records = _stable_records("pipe")
    result = detect_anomaly("pipe", records, "error_rate")
    assert result is not None
    assert result.is_anomaly is False


def test_detect_anomaly_flags_spike():
    records = _stable_records("pipe") + [_make_record("pipe", 0.99, 100.0)]
    result = detect_anomaly("pipe", records, "error_rate")
    assert result is not None
    assert result.is_anomaly is True
    assert result.metric == "error_rate"
    assert result.pipeline_name == "pipe"


def test_detect_anomaly_result_fields():
    records = _stable_records("pipe") + [_make_record("pipe", 0.99, 100.0)]
    result = detect_anomaly("pipe", records, "error_rate")
    assert result.current_value == pytest.approx(0.99)
    assert result.mean == pytest.approx(0.01)
    assert result.z_score > 2.5


def test_anomaly_result_summary_contains_pipeline_name():
    r = AnomalyResult(
        pipeline_name="my_pipe",
        metric="latency_ms",
        current_value=999.0,
        mean=100.0,
        std_dev=5.0,
        z_score=179.8,
        is_anomaly=True,
    )
    assert "my_pipe" in r.summary
    assert "latency_ms" in r.summary


# --- detect_all_anomalies ---

def test_detect_all_anomalies_empty_returns_empty():
    assert detect_all_anomalies([]) == []


def test_detect_all_anomalies_finds_spike():
    records = _stable_records("pipe") + [_make_record("pipe", 0.99, 100.0)]
    results = detect_all_anomalies(records)
    assert any(r.pipeline_name == "pipe" and r.metric == "error_rate" for r in results)


def test_detect_all_anomalies_stable_data_returns_empty():
    records = _stable_records("pipe")
    assert detect_all_anomalies(records) == []
