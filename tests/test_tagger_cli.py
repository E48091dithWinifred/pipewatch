"""Tests for pipewatch.tagger_cli."""
from __future__ import annotations

import json
import argparse
from pathlib import Path

import pytest

from pipewatch.tagger_cli import _load_statuses, cmd_tag
from pipewatch.checker import AlertLevel


@pytest.fixture
def status_file(tmp_path):
    data = [
        {"pipeline_name": "alpha", "level": "CRITICAL", "error_rate": 0.15, "latency_ms": 900.0},
        {"pipeline_name": "beta", "level": "WARNING", "error_rate": 0.04, "latency_ms": 300.0},
        {"pipeline_name": "gamma", "level": "OK", "error_rate": 0.0, "latency_ms": 100.0},
    ]
    p = tmp_path / "statuses.json"
    p.write_text(json.dumps(data))
    return p


@pytest.fixture
def config_file(tmp_path):
    content = """rules:\n  - level: CRITICAL\n    tags: [urgent, critical-alert]\n  - min_error_rate: 0.05\n    tags: [high-error]\n"""
    p = tmp_path / "tagger.yaml"
    p.write_text(content)
    return p


def test_load_statuses_returns_list(status_file):
    result = _load_statuses(str(status_file))
    assert len(result) == 3


def test_load_statuses_correct_level(status_file):
    result = _load_statuses(str(status_file))
    assert result[0].level == AlertLevel.CRITICAL


def test_load_statuses_missing_file():
    with pytest.raises(FileNotFoundError):
        _load_statuses("/nonexistent/path.json")


def _args(statuses, config, output="-", only_tagged=False):
    return argparse.Namespace(
        statuses=str(statuses),
        config=str(config),
        output=output,
        only_tagged=only_tagged,
    )


def test_cmd_tag_outputs_json(status_file, config_file, capsys):
    cmd_tag(_args(status_file, config_file))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert len(data) == 3


def test_cmd_tag_critical_has_urgent_tag(status_file, config_file, capsys):
    cmd_tag(_args(status_file, config_file))
    out = json.loads(capsys.readouterr().out)
    alpha = next(r for r in out if r["pipeline_name"] == "alpha")
    assert "urgent" in alpha["tags"]


def test_cmd_tag_only_tagged_filters_ok(status_file, config_file, capsys):
    cmd_tag(_args(status_file, config_file, only_tagged=True))
    out = json.loads(capsys.readouterr().out)
    names = [r["pipeline_name"] for r in out]
    assert "gamma" not in names


def test_cmd_tag_writes_to_file(status_file, config_file, tmp_path):
    out_path = tmp_path / "out.json"
    cmd_tag(_args(status_file, config_file, output=str(out_path)))
    assert out_path.exists()
    data = json.loads(out_path.read_text())
    assert len(data) == 3
