"""Tests for pipewatch.cli_dedupe."""

from __future__ import annotations

import argparse
import json
import os

import pytest

from pipewatch.checker import AlertLevel
from pipewatch.cli_dedupe import _load_statuses, cmd_dedupe


@pytest.fixture
def status_file(tmp_path):
    data = [
        {"pipeline": "alpha", "level": "warning", "message": "high error rate"},
        {"pipeline": "beta", "level": "ok", "message": ""},
    ]
    p = tmp_path / "statuses.json"
    p.write_text(json.dumps(data))
    return str(p)


def _args(statuses, state, min_repeat=3, output="text"):
    return argparse.Namespace(
        statuses=statuses,
        state=state,
        min_repeat=min_repeat,
        output=output,
    )


# --- _load_statuses ---

def test_load_statuses_returns_list(status_file):
    result = _load_statuses(status_file)
    assert len(result) == 2


def test_load_statuses_correct_level(status_file):
    result = _load_statuses(status_file)
    assert result[0].level == AlertLevel.WARNING
    assert result[1].level == AlertLevel.OK


def test_load_statuses_missing_file(tmp_path):
    with pytest.raises(SystemExit):
        _load_statuses(str(tmp_path / "nope.json"))


# --- cmd_dedupe ---

def test_cmd_dedupe_ok_always_passes(status_file, tmp_path, capsys):
    state = str(tmp_path / "state.json")
    args = _args(status_file, state, min_repeat=3, output="text")
    cmd_dedupe(args)
    out = capsys.readouterr().out
    # beta (OK) should always pass
    assert "beta" in out


def test_cmd_dedupe_warning_suppressed_first_run(status_file, tmp_path, capsys):
    state = str(tmp_path / "state.json")
    args = _args(status_file, state, min_repeat=3, output="text")
    cmd_dedupe(args)
    out = capsys.readouterr().out
    # alpha (WARNING) should be suppressed on first run
    assert "alpha" not in out


def test_cmd_dedupe_warning_fires_after_threshold(status_file, tmp_path, capsys):
    state = str(tmp_path / "state.json")
    # run 3 times to hit threshold
    for _ in range(3):
        cmd_dedupe(_args(status_file, state, min_repeat=3, output="text"))
    out = capsys.readouterr().out
    assert "alpha" in out


def test_cmd_dedupe_json_output(status_file, tmp_path, capsys):
    state = str(tmp_path / "state.json")
    # pre-seed 3 runs so alpha passes
    for _ in range(3):
        cmd_dedupe(_args(status_file, state, min_repeat=3, output="text"))
    capsys.readouterr()  # discard previous output
    cmd_dedupe(_args(status_file, state, min_repeat=3, output="json"))
    out = capsys.readouterr().out
    parsed = json.loads(out)
    names = [p["pipeline"] for p in parsed]
    assert "alpha" in names


def test_cmd_dedupe_no_alerts_message(tmp_path, capsys):
    # file with only OK statuses
    data = [{"pipeline": "x", "level": "ok", "message": ""}]
    sf = tmp_path / "s.json"
    sf.write_text(json.dumps(data))
    state = str(tmp_path / "state.json")
    # OK passes but "no alerts" message should not appear
    cmd_dedupe(_args(str(sf), state))
    out = capsys.readouterr().out
    assert "x" in out
