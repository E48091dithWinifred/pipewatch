"""Tests for pipewatch.cli_retry."""
import argparse
import os
import json
import pytest

from pipewatch.checker import AlertLevel
from pipewatch.retrier import record_attempt, _load_state
from pipewatch.cli_retry import cmd_retry, _make_policy


def _args(**kwargs):
    defaults = dict(
        state_file="",
        max_attempts=3,
        backoff=5.0,
        retry_on=["CRITICAL"],
        retry_cmd=None,
        pending_only=False,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


@pytest.fixture
def state_file(tmp_path):
    return str(tmp_path / "retry_state.json")


def test_list_no_state_file(state_file, capsys):
    args = _args(state_file=state_file, retry_cmd="list")
    rc = cmd_retry(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "No retry state" in out


def test_list_shows_entries(state_file, capsys):
    record_attempt("pipe_a", AlertLevel.CRITICAL, state_file, now=0.0)
    args = _args(state_file=state_file, retry_cmd="list")
    rc = cmd_retry(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "pipe_a" in out


def test_list_pending_only_excludes_resolved(state_file, capsys):
    from pipewatch.retrier import resolve
    record_attempt("pipe_a", AlertLevel.CRITICAL, state_file, now=0.0)
    record_attempt("pipe_b", AlertLevel.CRITICAL, state_file, now=0.0)
    resolve("pipe_a", state_file)
    args = _args(state_file=state_file, retry_cmd="list", pending_only=True)
    cmd_retry(args)
    out = capsys.readouterr().out
    assert "pipe_b" in out
    assert "pipe_a" not in out


def test_resolve_existing(state_file, capsys):
    record_attempt("pipe_c", AlertLevel.CRITICAL, state_file, now=0.0)
    args = _args(state_file=state_file, retry_cmd="resolve", pipeline="pipe_c")
    rc = cmd_retry(args)
    assert rc == 0
    state = _load_state(state_file)
    assert state["pipe_c"].resolved is True


def test_resolve_missing_returns_error(state_file, capsys):
    args = _args(state_file=state_file, retry_cmd="resolve", pipeline="ghost")
    rc = cmd_retry(args)
    assert rc == 1


def test_clear_removes_file(state_file, capsys):
    record_attempt("pipe_d", AlertLevel.CRITICAL, state_file, now=0.0)
    args = _args(state_file=state_file, retry_cmd="clear")
    rc = cmd_retry(args)
    assert rc == 0
    assert not os.path.exists(state_file)


def test_clear_no_file_ok(state_file, capsys):
    args = _args(state_file=state_file, retry_cmd="clear")
    rc = cmd_retry(args)
    assert rc == 0


def test_make_policy_uses_args():
    args = _args(max_attempts=5, backoff=30.0, retry_on=["WARNING", "CRITICAL"])
    policy = _make_policy(args)
    assert policy.max_attempts == 5
    assert policy.backoff_seconds == 30.0
    assert "WARNING" in policy.retry_on
