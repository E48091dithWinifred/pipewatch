"""Tests for pipewatch.cli_score."""
from __future__ import annotations

import json
import pytest

from pipewatch.checker import AlertLevel
from pipewatch.cli_score import _format_row, _load_statuses, cmd_score
from pipewatch.scorer import PipelineScore


# --- fixtures ---

@pytest.fixture()
def status_file(tmp_path):
    data = [
        {"pipeline": "ingest", "level": "OK", "error_rate": 0.01, "latency_ms": 120.0, "message": ""},
        {"pipeline": "transform", "level": "WARNING", "error_rate": 0.08, "latency_ms": 800.0, "message": "slow"},
        {"pipeline": "load", "level": "CRITICAL", "error_rate": 0.35, "latency_ms": 4000.0, "message": "critical"},
    ]
    p = tmp_path / "statuses.json"
    p.write_text(json.dumps(data))
    return str(p)


# --- _load_statuses ---

def test_load_statuses_returns_list(status_file):
    statuses = _load_statuses(status_file)
    assert len(statuses) == 3


def test_load_statuses_correct_level(status_file):
    statuses = _load_statuses(status_file)
    levels = {s.pipeline: s.level for s in statuses}
    assert levels["ingest"] == AlertLevel.OK
    assert levels["load"] == AlertLevel.CRITICAL


def test_load_statuses_missing_file():
    import pytest
    with pytest.raises(FileNotFoundError):
        _load_statuses("/nonexistent/path.json")


# --- _format_row ---

def _make_score(pipeline="pipe", score=95.0, grade_override=None) -> PipelineScore:
    ps = PipelineScore(
        pipeline=pipeline,
        score=score,
        level=AlertLevel.OK,
        error_rate=0.01,
        latency_ms=100.0,
    )
    return ps


def test_format_row_no_color_contains_pipeline():
    ps = _make_score(pipeline="my_pipe", score=88.5)
    row = _format_row(ps, color=False)
    assert "my_pipe" in row


def test_format_row_no_color_no_escape():
    ps = _make_score(score=95.0)
    row = _format_row(ps, color=False)
    assert "\033[" not in row


def test_format_row_color_contains_escape():
    ps = _make_score(score=95.0)
    row = _format_row(ps, color=True)
    assert "\033[" in row


def test_format_row_contains_score():
    ps = _make_score(score=72.3)
    row = _format_row(ps, color=False)
    assert "72.3" in row


# --- cmd_score ---

def test_cmd_score_prints_header(status_file, capsys):
    class Args:
        input = status_file
        no_color = True
        min_score = 0.0
        latency_ceiling = 5000.0
    cmd_score(Args())
    out = capsys.readouterr().out
    assert "Score" in out
    assert "Pipeline" in out


def test_cmd_score_prints_all_pipelines(status_file, capsys):
    class Args:
        input = status_file
        no_color = True
        min_score = 0.0
        latency_ceiling = 5000.0
    cmd_score(Args())
    out = capsys.readouterr().out
    assert "ingest" in out
    assert "transform" in out
    assert "load" in out


def test_cmd_score_min_score_filters(status_file, capsys):
    class Args:
        input = status_file
        no_color = True
        min_score = 50.0   # only show pipelines with score <= 50
        latency_ceiling = 5000.0
    cmd_score(Args())
    out = capsys.readouterr().out
    # 'load' (critical, high error rate) should appear; 'ingest' (ok) should not
    assert "load" in out
    assert "ingest" not in out


def test_cmd_score_exits_on_bad_file(tmp_path):
    class Args:
        input = str(tmp_path / "missing.json")
        no_color = True
        min_score = 0.0
        latency_ceiling = 5000.0
    with pytest.raises(SystemExit):
        cmd_score(Args())
