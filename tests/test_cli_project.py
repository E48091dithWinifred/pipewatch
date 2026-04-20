"""Tests for pipewatch.cli_project."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from pipewatch.cli_project import _load_history, _format_result, cmd_project
from pipewatch.projector import ProjectionPoint, ProjectionResult


@pytest.fixture()
def history_file(tmp_path: Path) -> Path:
    records = [
        {"pipeline": "etl_main", "timestamp": "2024-01-01T00:00:00", "level": "ok",
         "error_rate": 0.01, "latency_ms": 100.0, "rows_processed": 500},
        {"pipeline": "etl_main", "timestamp": "2024-01-01T01:00:00", "level": "ok",
         "error_rate": 0.02, "latency_ms": 110.0, "rows_processed": 510},
        {"pipeline": "etl_main", "timestamp": "2024-01-01T02:00:00", "level": "warning",
         "error_rate": 0.03, "latency_ms": 120.0, "rows_processed": 490},
    ]
    f = tmp_path / "history.json"
    f.write_text(json.dumps(records))
    return f


def _args(**kwargs) -> argparse.Namespace:
    defaults = {"history_file": "", "steps": 3, "pipeline": None, "as_json": False}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_load_history_returns_list(history_file: Path):
    records = _load_history(str(history_file))
    assert isinstance(records, list)
    assert len(records) == 3


def test_load_history_missing_returns_empty():
    records = _load_history("/nonexistent/history.json")
    assert records == []


def test_load_history_correct_pipeline(history_file: Path):
    records = _load_history(str(history_file))
    assert all(r.pipeline == "etl_main" for r in records)


def test_load_history_correct_error_rate(history_file: Path):
    records = _load_history(str(history_file))
    assert records[0].error_rate == pytest.approx(0.01)


def test_format_result_contains_pipeline():
    result = ProjectionResult(
        pipeline="my_pipe",
        steps=2,
        points=[
            ProjectionPoint(step=1, projected_error_rate=0.05, projected_latency_ms=150.0),
            ProjectionPoint(step=2, projected_error_rate=0.06, projected_latency_ms=160.0),
        ],
        degrading=True,
        improving=False,
    )
    text = _format_result(result)
    assert "my_pipe" in text


def test_format_result_contains_step_numbers():
    result = ProjectionResult(
        pipeline="my_pipe",
        steps=2,
        points=[
            ProjectionPoint(step=1, projected_error_rate=0.01, projected_latency_ms=100.0),
            ProjectionPoint(step=2, projected_error_rate=0.02, projected_latency_ms=110.0),
        ],
        degrading=False,
        improving=False,
    )
    text = _format_result(result)
    assert "step 1" in text
    assert "step 2" in text


def test_cmd_project_prints_output(history_file: Path, capsys):
    cmd_project(_args(history_file=str(history_file), steps=2))
    captured = capsys.readouterr()
    assert "etl_main" in captured.out


def test_cmd_project_json_output_is_valid(history_file: Path, capsys):
    cmd_project(_args(history_file=str(history_file), steps=2, as_json=True))
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "etl_main"


def test_cmd_project_json_contains_points(history_file: Path, capsys):
    cmd_project(_args(history_file=str(history_file), steps=2, as_json=True))
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert len(data[0]["points"]) == 2


def test_cmd_project_filter_by_pipeline(history_file: Path, capsys):
    cmd_project(_args(history_file=str(history_file), pipeline="etl_main", steps=2))
    captured = capsys.readouterr()
    assert "etl_main" in captured.out


def test_cmd_project_missing_file_prints_error(capsys):
    cmd_project(_args(history_file="/no/such/file.json", steps=2))
    captured = capsys.readouterr()
    assert "No history" in captured.err
