"""tests/test_cli_sample.py — Tests for pipewatch.cli_sample."""
import json
import argparse
import pytest
from pathlib import Path

from pipewatch.cli_sample import _load_statuses, cmd_sample
from pipewatch.checker import AlertLevel


@pytest.fixture
def status_file(tmp_path):
    data = [
        {"pipeline_name": "alpha", "level": "OK", "error_rate": 0.01, "latency_ms": 120.0, "message": ""},
        {"pipeline_name": "beta",  "level": "WARNING", "error_rate": 0.08, "latency_ms": 300.0, "message": "slow"},
        {"pipeline_name": "gamma", "level": "CRITICAL", "error_rate": 0.25, "latency_ms": 900.0, "message": "err"},
    ]
    f = tmp_path / "statuses.json"
    f.write_text(json.dumps(data))
    return str(f)


def _args(**kwargs) -> argparse.Namespace:
    defaults = dict(input="", rate=1.0, seed=None, min_keep=0, output=None, summary=False)
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_load_statuses_returns_list(status_file):
    result = _load_statuses(status_file)
    assert len(result) == 3


def test_load_statuses_correct_level(status_file):
    result = _load_statuses(status_file)
    assert result[1].level == AlertLevel.WARNING


def test_load_statuses_missing_file(capsys):
    result = _load_statuses("/nonexistent/path.json")
    assert result == []
    captured = capsys.readouterr()
    assert "not found" in captured.err


def test_cmd_sample_rate_one_prints_all(status_file, capsys):
    cmd_sample(_args(input=status_file, rate=1.0))
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out
    assert "gamma" in out


def test_cmd_sample_summary_flag(status_file, capsys):
    cmd_sample(_args(input=status_file, rate=1.0, summary=True))
    out = capsys.readouterr().out
    assert "3/3" in out


def test_cmd_sample_writes_output_file(status_file, tmp_path):
    out_file = str(tmp_path / "out.json")
    cmd_sample(_args(input=status_file, rate=1.0, output=out_file))
    data = json.loads(Path(out_file).read_text())
    assert len(data) == 3


def test_cmd_sample_invalid_rate_exits(status_file):
    with pytest.raises(SystemExit):
        cmd_sample(_args(input=status_file, rate=2.5))


def test_cmd_sample_no_statuses(tmp_path, capsys):
    empty = tmp_path / "empty.json"
    empty.write_text("[]")
    cmd_sample(_args(input=str(empty)))
    out = capsys.readouterr().out
    assert "No statuses" in out


def test_cmd_sample_deterministic(status_file, tmp_path):
    out1 = str(tmp_path / "out1.json")
    out2 = str(tmp_path / "out2.json")
    cmd_sample(_args(input=status_file, rate=0.5, seed=42, output=out1))
    cmd_sample(_args(input=status_file, rate=0.5, seed=42, output=out2))
    d1 = json.loads(Path(out1).read_text())
    d2 = json.loads(Path(out2).read_text())
    assert [x["pipeline_name"] for x in d1] == [x["pipeline_name"] for x in d2]
