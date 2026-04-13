"""Tests for pipewatch.cli_validate."""
import json
import sys
import pytest
from pathlib import Path

from pipewatch.cli_validate import _load_statuses, cmd_validate
from pipewatch.checker import AlertLevel


@pytest.fixture
def status_file(tmp_path):
    data = [
        {
            "pipeline_name": "pipe_ok",
            "level": "ok",
            "message": "all good",
            "error_rate": 0.01,
            "latency_ms": 120.0,
        },
        {
            "pipeline_name": "pipe_bad",
            "level": "critical",
            "message": "down",
            "error_rate": 0.45,
            "latency_ms": 9000.0,
        },
    ]
    f = tmp_path / "statuses.json"
    f.write_text(json.dumps(data))
    return str(f)


def _args(**kwargs):
    import argparse
    defaults = dict(
        statuses=None,
        max_error_rate=None,
        max_latency_ms=None,
        forbidden_levels=[],
        fail_fast=False,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_load_statuses_returns_list(status_file):
    statuses = _load_statuses(status_file)
    assert len(statuses) == 2


def test_load_statuses_correct_level(status_file):
    statuses = _load_statuses(status_file)
    levels = {s.pipeline_name: s.level for s in statuses}
    assert levels["pipe_ok"] == AlertLevel.OK
    assert levels["pipe_bad"] == AlertLevel.CRITICAL


def test_load_statuses_missing_file():
    with pytest.raises(FileNotFoundError):
        _load_statuses("/nonexistent/path.json")


def test_cmd_validate_all_pass(status_file, capsys):
    args = _args(statuses=status_file)
    cmd_validate(args)
    out = capsys.readouterr().out
    assert out.count("PASS") == 2


def test_cmd_validate_error_rate_violation(status_file, capsys):
    args = _args(statuses=status_file, max_error_rate=0.05)
    cmd_validate(args)
    out = capsys.readouterr().out
    assert "FAIL" in out
    assert "pipe_bad" in out


def test_cmd_validate_latency_violation(status_file, capsys):
    args = _args(statuses=status_file, max_latency_ms=500.0)
    cmd_validate(args)
    out = capsys.readouterr().out
    assert "FAIL" in out


def test_cmd_validate_forbidden_level_violation(status_file, capsys):
    args = _args(statuses=status_file, forbidden_levels=["critical"])
    cmd_validate(args)
    out = capsys.readouterr().out
    assert "FAIL" in out
    assert "forbidden" in out


def test_cmd_validate_fail_fast_exits(status_file):
    args = _args(statuses=status_file, max_error_rate=0.05, fail_fast=True)
    with pytest.raises(SystemExit) as exc_info:
        cmd_validate(args)
    assert exc_info.value.code == 1


def test_cmd_validate_no_violation_no_exit(status_file):
    args = _args(statuses=status_file, fail_fast=True)
    # Should not raise
    cmd_validate(args)
