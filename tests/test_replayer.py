"""Tests for pipewatch.replayer."""
import json
import pytest
from pathlib import Path
from pipewatch.replayer import ReplayConfig, ReplayResult, load_replay_records, replay


def _make_record(name: str, level: str, error_rate: float = 0.0) -> dict:
    return {
        "pipeline_name": name,
        "checked_at": "2024-01-01T00:00:00",
        "level": level,
        "error_rate": error_rate,
        "latency_ms": 100.0,
    }


@pytest.fixture
def history_file(tmp_path: Path) -> Path:
    p = tmp_path / "history.json"
    records = [
        _make_record("pipe_a", "ok"),
        _make_record("pipe_b", "warning", 0.05),
        _make_record("pipe_a", "critical", 0.2),
        _make_record("pipe_a", "ok"),
    ]
    p.write_text(json.dumps(records))
    return p


def test_load_replay_records_all(history_file):
    cfg = ReplayConfig(history_file=str(history_file))
    records = load_replay_records(cfg)
    assert len(records) == 4


def test_load_replay_records_filtered_by_name(history_file):
    cfg = ReplayConfig(history_file=str(history_file), pipeline_name="pipe_a")
    records = load_replay_records(cfg)
    assert all(r.pipeline_name == "pipe_a" for r in records)
    assert len(records) == 3


def test_load_replay_records_max_runs(history_file):
    cfg = ReplayConfig(history_file=str(history_file), max_runs=2)
    records = load_replay_records(cfg)
    assert len(records) == 2


def test_load_replay_records_reverse(history_file):
    cfg = ReplayConfig(history_file=str(history_file), pipeline_name="pipe_a", reverse=True)
    records = load_replay_records(cfg)
    assert records[0].level == "ok"
    assert records[-1].level == "ok"


def test_replay_returns_result(history_file):
    cfg = ReplayConfig(history_file=str(history_file), pipeline_name="pipe_a")
    result = replay(cfg)
    assert isinstance(result, ReplayResult)
    assert result.total_replayed == 3


def test_replay_calls_on_record(history_file):
    cfg = ReplayConfig(history_file=str(history_file))
    seen = []
    replay(cfg, on_record=seen.append)
    assert len(seen) == 4


def test_replay_result_summary(history_file):
    cfg = ReplayConfig(history_file=str(history_file), pipeline_name="pipe_a")
    result = replay(cfg)
    assert "pipe_a" in result.summary()
    assert "3" in result.summary()


def test_replay_missing_file_returns_empty(tmp_path):
    cfg = ReplayConfig(history_file=str(tmp_path / "missing.json"))
    result = replay(cfg)
    assert result.total_replayed == 0
    assert result.records == []
