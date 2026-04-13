"""Tests for pipewatch.digest and the digest reporter helpers."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone

import pytest

from pipewatch.digest import Digest, PipelineDigest, build_digest
from pipewatch.history import RunRecord, append_run
from pipewatch.reporter import format_digest_line, print_digest


@pytest.fixture()
def history_file(tmp_path):
    return str(tmp_path / "history.json")


def _write_record(path: str, name: str, level: str, error_rate: float, latency_ms: float) -> None:
    rec = RunRecord(
        pipeline_name=name,
        timestamp=datetime.now(timezone.utc).isoformat(),
        alert_level=level,
        error_rate=error_rate,
        latency_ms=latency_ms,
        rows_processed=1000,
    )
    append_run(path, rec)


def test_build_digest_empty_history(history_file):
    digest = build_digest(["pipe_a"], history_file)
    assert isinstance(digest, Digest)
    assert len(digest.pipelines) == 1
    pd = digest.pipelines[0]
    assert pd.pipeline_name == "pipe_a"
    assert pd.total_runs == 0


def test_build_digest_counts_levels(history_file):
    _write_record(history_file, "pipe_a", "ok", 0.01, 120.0)
    _write_record(history_file, "pipe_a", "warning", 0.05, 200.0)
    _write_record(history_file, "pipe_a", "critical", 0.15, 500.0)

    digest = build_digest(["pipe_a"], history_file)
    pd = digest.pipelines[0]
    assert pd.total_runs == 3
    assert pd.ok_count == 1
    assert pd.warning_count == 1
    assert pd.critical_count == 1


def test_build_digest_avg_error_rate(history_file):
    _write_record(history_file, "pipe_b", "ok", 0.02, 100.0)
    _write_record(history_file, "pipe_b", "ok", 0.04, 100.0)

    digest = build_digest(["pipe_b"], history_file)
    pd = digest.pipelines[0]
    assert abs(pd.avg_error_rate - 0.03) < 1e-6


def test_health_score_all_ok(history_file):
    for _ in range(5):
        _write_record(history_file, "pipe_c", "ok", 0.01, 100.0)
    digest = build_digest(["pipe_c"], history_file)
    assert digest.pipelines[0].health_score == 100.0


def test_health_score_penalises_criticals(history_file):
    _write_record(history_file, "pipe_d", "critical", 0.2, 600.0)
    digest = build_digest(["pipe_d"], history_file)
    assert digest.pipelines[0].health_score < 100.0


def test_most_critical_returns_lowest_score(history_file):
    _write_record(history_file, "pipe_e", "ok", 0.01, 100.0)
    _write_record(history_file, "pipe_f", "critical", 0.2, 600.0)

    digest = build_digest(["pipe_e", "pipe_f"], history_file)
    assert digest.most_critical is not None
    assert digest.most_critical.pipeline_name == "pipe_f"


def test_format_digest_line_contains_name(history_file):
    _write_record(history_file, "pipe_g", "ok", 0.01, 100.0)
    digest = build_digest(["pipe_g"], history_file)
    line = format_digest_line(digest.pipelines[0])
    assert "pipe_g" in line
    assert "runs=" in line


def test_print_digest_runs_without_error(history_file, capsys):
    _write_record(history_file, "pipe_h", "ok", 0.01, 100.0)
    digest = build_digest(["pipe_h"], history_file)
    print_digest(digest)
    captured = capsys.readouterr()
    assert "Digest" in captured.out
    assert "pipe_h" in captured.out
