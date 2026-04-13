"""Tests for pipewatch.cli_anomaly."""
import argparse
import json
from pathlib import Path

import pytest

from pipewatch.cli_anomaly import _build_anomaly_parser, _load_history, cmd_anomaly
from pipewatch.history import RunRecord, _save_records


def _make_record(name: str, error_rate: float, latency_ms: float) -> RunRecord:
    return RunRecord(
        pipeline_name=name,
        timestamp="2024-01-01T00:00:00",
        level="OK",
        error_rate=error_rate,
        latency_ms=latency_ms,
        rows_processed=1000,
    )


@pytest.fixture
def history_file(tmp_path: Path) -> Path:
    p = tmp_path / "history.json"
    stable = [_make_record("pipe_a", 0.01, 100.0) for _ in range(6)]
    spike = [_make_record("pipe_a", 0.99, 100.0)]
    _save_records(p, stable + spike)
    return p


# --- _load_history ---

def test_load_history_missing_returns_empty(tmp_path: Path):
    result = _load_history(str(tmp_path / "missing.json"))
    assert result == []


def test_load_history_returns_records(history_file: Path):
    result = _load_history(str(history_file))
    assert len(result) == 7


# --- cmd_anomaly ---

def _args(**kwargs) -> argparse.Namespace:
    defaults = {
        "history": ".pipewatch_history.json",
        "pipeline": None,
        "metric": None,
        "threshold": 2.5,
        "output_json": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_cmd_anomaly_no_history_returns_1(tmp_path: Path):
    args = _args(history=str(tmp_path / "missing.json"))
    assert cmd_anomaly(args) == 1


def test_cmd_anomaly_detects_spike(history_file: Path, capsys):
    args = _args(history=str(history_file))
    rc = cmd_anomaly(args)
    assert rc == 0
    captured = capsys.readouterr()
    assert "pipe_a" in captured.out
    assert "error_rate" in captured.out


def test_cmd_anomaly_no_anomaly_stable(tmp_path: Path, capsys):
    p = tmp_path / "history.json"
    stable = [_make_record("pipe_b", 0.01, 100.0) for _ in range(6)]
    _save_records(p, stable)
    args = _args(history=str(p))
    rc = cmd_anomaly(args)
    assert rc == 0
    assert "No anomalies" in capsys.readouterr().out


def test_cmd_anomaly_json_output(history_file: Path, capsys):
    args = _args(history=str(history_file), output_json=True)
    rc = cmd_anomaly(args)
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert all("pipeline" in item for item in data)


def test_cmd_anomaly_filter_by_pipeline(history_file: Path, capsys):
    args = _args(history=str(history_file), pipeline="pipe_a", metric="error_rate")
    rc = cmd_anomaly(args)
    assert rc == 0
    assert "pipe_a" in capsys.readouterr().out


def test_build_anomaly_parser_returns_parser():
    main_parser = argparse.ArgumentParser()
    sub = main_parser.add_subparsers()
    p = _build_anomaly_parser(sub)
    assert p is not None
