import json
import pytest
from pathlib import Path
from unittest.mock import patch
from pipewatch.cli_zip import _load_statuses, cmd_zip
from pipewatch.checker import AlertLevel


@pytest.fixture
def status_file(tmp_path):
    data = [
        {"pipeline_name": "alpha", "level": "OK", "message": "", "error_rate": 0.0, "latency_ms": 50.0},
        {"pipeline_name": "beta", "level": "WARNING", "message": "slow", "error_rate": 0.05, "latency_ms": 300.0},
    ]
    p = tmp_path / "statuses.json"
    p.write_text(json.dumps(data))
    return str(p)


def test_load_statuses_returns_list(status_file):
    result = _load_statuses(status_file)
    assert len(result) == 2


def test_load_statuses_correct_level(status_file):
    result = _load_statuses(status_file)
    assert result[0].level == AlertLevel.OK
    assert result[1].level == AlertLevel.WARNING


def test_load_statuses_missing_file():
    result = _load_statuses("/nonexistent/path.json")
    assert result == []


class _Args:
    def __init__(self, left, right, only_diff=False):
        self.left = left
        self.right = right
        self.only_diff = only_diff


def test_cmd_zip_prints_summary(status_file, capsys):
    args = _Args(left=status_file, right=status_file)
    cmd_zip(args)
    out = capsys.readouterr().out
    assert "total=" in out


def test_cmd_zip_shows_pipeline_names(status_file, capsys):
    args = _Args(left=status_file, right=status_file)
    cmd_zip(args)
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out


def test_cmd_zip_only_diff_hides_matching(status_file, capsys):
    args = _Args(left=status_file, right=status_file, only_diff=True)
    cmd_zip(args)
    out = capsys.readouterr().out
    # same file => no diffs, only summary line
    assert "alpha" not in out


def test_cmd_zip_diff_marker(tmp_path, capsys):
    left_data = [{"pipeline_name": "alpha", "level": "OK", "message": "", "error_rate": 0.0, "latency_ms": 10.0}]
    right_data = [{"pipeline_name": "alpha", "level": "CRITICAL", "message": "", "error_rate": 0.5, "latency_ms": 10.0}]
    lf = tmp_path / "left.json"
    rf = tmp_path / "right.json"
    lf.write_text(json.dumps(left_data))
    rf.write_text(json.dumps(right_data))
    args = _Args(left=str(lf), right=str(rf))
    cmd_zip(args)
    out = capsys.readouterr().out
    assert "~~" in out
