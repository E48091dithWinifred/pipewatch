"""Tests for pipewatch.cli_scale."""
from __future__ import annotations

import json
import argparse
from pathlib import Path

import pytest

from pipewatch.checker import AlertLevel
from pipewatch.cli_scale import _build_scale_parser, _load_statuses, _format_row, cmd_scale
from pipewatch.scaler import ScaledStatus


@pytest.fixture()
def status_file(tmp_path: Path) -> Path:
    data = [
        {"pipeline_name": "alpha", "level": "ok", "error_rate": 0.1, "latency_ms": 200.0, "message": ""},
        {"pipeline_name": "beta", "level": "warning", "error_rate": 0.4, "latency_ms": 1500.0, "message": "slow"},
        {"pipeline_name": "gamma", "level": "critical", "error_rate": 0.9, "latency_ms": 8000.0, "message": "down"},
    ]
    p = tmp_path / "statuses.json"
    p.write_text(json.dumps(data))
    return p


def _args(**kwargs) -> argparse.Namespace:
    defaults = {
        "input": "statuses.json",
        "max_error_rate": 1.0,
        "max_latency_ms": 10_000.0,
        "output": "table",
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# --- _load_statuses ---

def test_load_statuses_returns_list(status_file: Path):
    result = _load_statuses(str(status_file))
    assert len(result) == 3


def test_load_statuses_correct_level(status_file: Path):
    result = _load_statuses(str(status_file))
    assert result[1].level == AlertLevel.WARNING


def test_load_statuses_missing_file(tmp_path: Path):
    result = _load_statuses(str(tmp_path / "nope.json"))
    assert result == []


# --- _format_row ---

def test_format_row_contains_name():
    s = ScaledStatus(name="mypipe", level="ok", error_rate_scaled=0.1, latency_scaled=0.2, composite_score=0.15)
    assert "mypipe" in _format_row(s)


def test_format_row_contains_level():
    s = ScaledStatus(name="p", level="critical", error_rate_scaled=1.0, latency_scaled=1.0, composite_score=1.0)
    assert "critical" in _format_row(s)


# --- cmd_scale table output ---

def test_cmd_scale_table_prints_header(status_file: Path, capsys):
    cmd_scale(_args(input=str(status_file)))
    out = capsys.readouterr().out
    assert "PIPELINE" in out
    assert "COMPOSITE" in out


def test_cmd_scale_table_prints_pipeline_names(status_file: Path, capsys):
    cmd_scale(_args(input=str(status_file)))
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out
    assert "gamma" in out


def test_cmd_scale_table_correct_row_count(status_file: Path, capsys):
    cmd_scale(_args(input=str(status_file)))
    out = capsys.readouterr().out
    # header + separator + 3 data rows
    lines = [l for l in out.splitlines() if l.strip()]
    assert len(lines) == 5


# --- cmd_scale json output ---

def test_cmd_scale_json_output_is_valid(status_file: Path, capsys):
    cmd_scale(_args(input=str(status_file), output="json"))
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert isinstance(parsed, list)
    assert len(parsed) == 3


def test_cmd_scale_json_contains_composite_score(status_file: Path, capsys):
    cmd_scale(_args(input=str(status_file), output="json"))
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert "composite_score" in parsed[0]


def test_cmd_scale_json_values_clamped(status_file: Path, capsys):
    cmd_scale(_args(input=str(status_file), max_error_rate=0.05, max_latency_ms=50.0, output="json"))
    out = capsys.readouterr().out
    parsed = json.loads(out)
    for row in parsed:
        assert row["error_rate_scaled"] <= 1.0
        assert row["latency_scaled"] <= 1.0


# --- missing file ---

def test_cmd_scale_missing_file_prints_nothing(tmp_path: Path, capsys):
    cmd_scale(_args(input=str(tmp_path / "missing.json")))
    out = capsys.readouterr().out
    assert "No statuses" in out
