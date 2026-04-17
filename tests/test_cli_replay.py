"""Tests for pipewatch.cli_replay."""
import json
import argparse
import pytest
from pathlib import Path
from pipewatch.cli_replay import cmd_replay, _build_replay_parser


def _make_record(name: str, level: str) -> dict:
    return {
        "pipeline_name": name,
        "checked_at": "2024-06-01T12:00:00",
        "level": level,
        "error_rate": 0.01,
        "latency_ms": 120.0,
    }


@pytest.fixture
def history_file(tmp_path: Path) -> Path:
    p = tmp_path / "history.json"
    p.write_text(json.dumps([
        _make_record("alpha", "ok"),
        _make_record("beta", "warning"),
        _make_record("alpha", "critical"),
    ]))
    return p


def _args(history_file, pipeline=None, max_runs=50, reverse=False, as_json=False):
    return argparse.Namespace(
        history_file=str(history_file),
        pipeline=pipeline,
        max_runs=max_runs,
        reverse=reverse,
        as_json=as_json,
    )


def test_cmd_replay_prints_summary(history_file, capsys):
    cmd_replay(_args(history_file))
    out = capsys.readouterr().out
    assert "Replayed" in out


def test_cmd_replay_filters_pipeline(history_file, capsys):
    cmd_replay(_args(history_file, pipeline="alpha"))
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" not in out


def test_cmd_replay_json_output(history_file, capsys):
    cmd_replay(_args(history_file, as_json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert len(data) == 3


def test_cmd_replay_json_has_fields(history_file, capsys):
    cmd_replay(_args(history_file, as_json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert "pipeline_name" in data[0]
    assert "level" in data[0]


def test_cmd_replay_max_runs(history_file, capsys):
    cmd_replay(_args(history_file, max_runs=1, as_json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert len(data) == 1


def test_build_replay_parser_registers_args():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    replay_parser = sub.add_parser("replay")
    _build_replay_parser(replay_parser)
    args = replay_parser.parse_args(["some_file.json", "--pipeline", "mypipe", "--max-runs", "10"])
    assert args.pipeline == "mypipe"
    assert args.max_runs == 10
