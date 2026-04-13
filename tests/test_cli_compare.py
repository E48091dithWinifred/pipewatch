"""Tests for pipewatch.cli_compare."""
import json
import sys
from pathlib import Path

import pytest

from pipewatch.cli_compare import _load_statuses, cmd_compare
from pipewatch.checker import AlertLevel


@pytest.fixture()
def status_file(tmp_path):
    """Write a minimal status JSON and return a factory."""
    def _write(name: str, records: list) -> str:
        p = tmp_path / name
        p.write_text(json.dumps(records))
        return str(p)
    return _write


SAMPLE_RECORDS = [
    {"pipeline_name": "alpha", "level": "ok", "error_rate": 0.01, "latency_ms": 120.0, "message": ""},
    {"pipeline_name": "beta", "level": "warning", "error_rate": 0.08, "latency_ms": 300.0, "message": "slow"},
]


def test_load_statuses_returns_list(status_file):
    path = status_file("prev.json", SAMPLE_RECORDS)
    statuses = _load_statuses(path)
    assert len(statuses) == 2


def test_load_statuses_correct_level(status_file):
    path = status_file("prev.json", SAMPLE_RECORDS)
    statuses = _load_statuses(path)
    assert statuses[1].level == AlertLevel.WARNING


def test_cmd_compare_no_changes(status_file, capsys):
    path = status_file("a.json", SAMPLE_RECORDS)
    path2 = status_file("b.json", SAMPLE_RECORDS)

    class Args:
        previous = path
        current = path2
        regressions_only = False

    cmd_compare(Args())
    out = capsys.readouterr().out
    assert "No status changes" in out


def test_cmd_compare_detects_degradation(status_file, capsys):
    prev = [{"pipeline_name": "alpha", "level": "ok", "error_rate": 0.01, "latency_ms": 100.0, "message": ""}]
    curr = [{"pipeline_name": "alpha", "level": "critical", "error_rate": 0.3, "latency_ms": 100.0, "message": ""}]
    p1 = status_file("p.json", prev)
    p2 = status_file("c.json", curr)

    class Args:
        previous = p1
        current = p2
        regressions_only = False

    cmd_compare(Args())
    out = capsys.readouterr().out
    assert "OK" in out or "CRITICAL" in out


def test_cmd_compare_regressions_only_exits(status_file):
    prev = [{"pipeline_name": "alpha", "level": "ok", "error_rate": 0.01, "latency_ms": 100.0, "message": ""}]
    curr = [{"pipeline_name": "alpha", "level": "critical", "error_rate": 0.3, "latency_ms": 100.0, "message": ""}]
    p1 = status_file("p.json", prev)
    p2 = status_file("c.json", curr)

    class Args:
        previous = p1
        current = p2
        regressions_only = True

    with pytest.raises(SystemExit) as exc:
        cmd_compare(Args())
    assert exc.value.code == 1


def test_cmd_compare_regressions_only_no_exit_when_ok(status_file):
    p1 = status_file("p.json", SAMPLE_RECORDS)
    p2 = status_file("c.json", SAMPLE_RECORDS)

    class Args:
        previous = p1
        current = p2
        regressions_only = True

    cmd_compare(Args())  # should not raise
