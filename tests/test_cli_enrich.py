"""Tests for pipewatch.cli_enrich."""
import json
import argparse
from pathlib import Path

import pytest

from pipewatch.cli_enrich import _load_statuses, cmd_enrich
from pipewatch.checker import AlertLevel


@pytest.fixture
def status_file(tmp_path) -> Path:
    data = [
        {"name": "etl.orders", "level": "ok", "message": "", "error_rate": 0.0, "latency_ms": 80.0},
        {"name": "stream.events", "level": "warning", "message": "slow", "error_rate": 0.1, "latency_ms": 500.0},
    ]
    f = tmp_path / "statuses.json"
    f.write_text(json.dumps(data))
    return f


def _args(**kwargs) -> argparse.Namespace:
    defaults = {
        "input": "",
        "prefix_tags": [],
        "owners": [],
        "env": "production",
        "output": "-",
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_load_statuses_returns_list(status_file):
    result = _load_statuses(str(status_file))
    assert len(result) == 2


def test_load_statuses_correct_level(status_file):
    result = _load_statuses(str(status_file))
    assert result[1].level == AlertLevel.WARNING


def test_load_statuses_missing_file():
    result = _load_statuses("/nonexistent/path.json")
    assert result == []


def test_cmd_enrich_stdout(status_file, capsys):
    a = _args(input=str(status_file))
    cmd_enrich(a)
    out = capsys.readouterr().out
    data = json.loads(out)
    assert len(data) == 2


def test_cmd_enrich_tags_applied(status_file, capsys):
    a = _args(
        input=str(status_file),
        prefix_tags=["etl.:batch", "etl.:internal"],
    )
    cmd_enrich(a)
    out = capsys.readouterr().out
    data = json.loads(out)
    etl_entry = next(d for d in data if d["name"] == "etl.orders")
    assert "batch" in etl_entry["tags"]
    assert "internal" in etl_entry["tags"]


def test_cmd_enrich_owner_applied(status_file, capsys):
    a = _args(
        input=str(status_file),
        owners=["etl.:data-eng"],
    )
    cmd_enrich(a)
    out = capsys.readouterr().out
    data = json.loads(out)
    etl_entry = next(d for d in data if d["name"] == "etl.orders")
    assert etl_entry["owner"] == "data-eng"


def test_cmd_enrich_environment_set(status_file, capsys):
    a = _args(input=str(status_file), env="staging")
    cmd_enrich(a)
    out = capsys.readouterr().out
    data = json.loads(out)
    assert all(d["environment"] == "staging" for d in data)


def test_cmd_enrich_writes_file(status_file, tmp_path):
    out_file = tmp_path / "enriched.json"
    a = _args(input=str(status_file), output=str(out_file))
    cmd_enrich(a)
    assert out_file.exists()
    data = json.loads(out_file.read_text())
    assert len(data) == 2
