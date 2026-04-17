"""Tests for pipewatch/cli_tail.py"""
import json
import pytest
from pathlib import Path
from pipewatch.cli_tail import cmd_tail, _build_tail_parser


def _make_record(pipeline: str, level: str, ts: str) -> dict:
    return {"pipeline": pipeline, "level": level, "timestamp": ts,
            "error_rate": 0.01, "latency_ms": 200.0, "rows_processed": 500}


@pytest.fixture
def history_file(tmp_path: Path) -> str:
    records = [
        _make_record("alpha", "ok", "2024-03-01T10:00:00"),
        _make_record("beta", "warning", "2024-03-01T10:01:00"),
        _make_record("alpha", "critical", "2024-03-01T10:02:00"),
    ]
    p = tmp_path / "hist.json"
    p.write_text(json.dumps(records))
    return str(p)


def _args(history, count=10, pipeline=None, as_json=False):
    parser = _build_tail_parser()
    parts = [history, "-n", str(count)]
    if pipeline:
        parts += ["--pipeline", pipeline]
    if as_json:
        parts += ["--json"]
    return parser.parse_args(parts)


def test_cmd_tail_prints_summary(history_file, capsys):
    cmd_tail(_args(history_file, count=3))
    out = capsys.readouterr().out
    assert "record" in out


def test_cmd_tail_shows_pipeline_names(history_file, capsys):
    cmd_tail(_args(history_file, count=10))
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out


def test_cmd_tail_filter_pipeline(history_file, capsys):
    cmd_tail(_args(history_file, count=10, pipeline="alpha"))
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" not in out.split("\n", 1)[1]  # skip summary line


def test_cmd_tail_json_output(history_file, capsys):
    cmd_tail(_args(history_file, count=2, as_json=True))
    out = capsys.readouterr().out
    lines = out.strip().split("\n", 1)
    parsed = json.loads(lines[1])
    assert isinstance(parsed, list)
    assert len(parsed) == 2


def test_cmd_tail_json_has_expected_keys(history_file, capsys):
    cmd_tail(_args(history_file, count=1, as_json=True))
    out = capsys.readouterr().out
    lines = out.strip().split("\n", 1)
    parsed = json.loads(lines[1])
    assert "pipeline" in parsed[0]
    assert "level" in parsed[0]
    assert "timestamp" in parsed[0]


def test_cmd_tail_missing_file_prints_zero(tmp_path, capsys):
    missing = str(tmp_path / "nope.json")
    cmd_tail(_args(missing, count=5))
    out = capsys.readouterr().out
    assert "0" in out
