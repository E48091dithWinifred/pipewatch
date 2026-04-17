"""Tests for pipewatch/tailer.py"""
import json
import pytest
from pathlib import Path
from pipewatch.tailer import TailConfig, TailResult, tail_history


def _make_record(pipeline: str, level: str, ts: str) -> dict:
    return {"pipeline": pipeline, "level": level, "timestamp": ts,
            "error_rate": 0.0, "latency_ms": 100.0, "rows_processed": 1000}


@pytest.fixture
def history_file(tmp_path: Path) -> str:
    records = [
        _make_record("pipe_a", "ok", "2024-01-01T00:00:00"),
        _make_record("pipe_b", "warning", "2024-01-01T00:01:00"),
        _make_record("pipe_a", "critical", "2024-01-01T00:02:00"),
        _make_record("pipe_a", "ok", "2024-01-01T00:03:00"),
        _make_record("pipe_b", "ok", "2024-01-01T00:04:00"),
    ]
    p = tmp_path / "history.json"
    p.write_text(json.dumps(records))
    return str(p)


def test_tail_config_defaults():
    cfg = TailConfig()
    assert cfg.n == 10
    assert cfg.pipeline is None


def test_tail_config_invalid_raises():
    with pytest.raises(ValueError):
        TailConfig(n=0)


def test_tail_result_count(history_file):
    result = tail_history(history_file, TailConfig(n=3))
    assert result.count == 3


def test_tail_result_returns_last_n(history_file):
    result = tail_history(history_file, TailConfig(n=2))
    assert result.records[0].pipeline == "pipe_a"
    assert result.records[1].pipeline == "pipe_b"


def test_tail_filter_by_pipeline(history_file):
    result = tail_history(history_file, TailConfig(n=10, pipeline="pipe_a"))
    assert all(r.pipeline == "pipe_a" for r in result.records)
    assert result.count == 3


def test_tail_filter_n_limits_filtered(history_file):
    result = tail_history(history_file, TailConfig(n=2, pipeline="pipe_a"))
    assert result.count == 2


def test_tail_missing_file_returns_empty(tmp_path):
    result = tail_history(str(tmp_path / "missing.json"), TailConfig(n=5))
    assert result.count == 0


def test_tail_summary_no_filter(history_file):
    result = tail_history(history_file, TailConfig(n=3))
    assert "3" in result.summary()
    assert "record" in result.summary()


def test_tail_summary_with_pipeline(history_file):
    result = tail_history(history_file, TailConfig(n=5, pipeline="pipe_b"))
    assert "pipe_b" in result.summary()
