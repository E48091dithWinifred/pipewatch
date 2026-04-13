"""Tests for pipewatch.exporter."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.digest import Digest, PipelineDigest
from pipewatch.exporter import export_digest, export_statuses


def _make_status(
    name: str = "pipe_a",
    level: AlertLevel = AlertLevel.OK,
    message: str = "all good",
    error_rate: float = 0.01,
    latency_ms: float = 120.0,
) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message=message,
        error_rate=error_rate,
        latency_ms=latency_ms,
    )


@pytest.fixture()
def sample_statuses():
    return [
        _make_status("pipe_a", AlertLevel.OK, "ok", 0.01, 100.0),
        _make_status("pipe_b", AlertLevel.WARNING, "slow", 0.05, 800.0),
        _make_status("pipe_c", AlertLevel.CRITICAL, "high errors", 0.25, 2000.0),
    ]


@pytest.fixture()
def sample_digest():
    pipelines = [
        PipelineDigest(
            pipeline_name="pipe_a",
            health_score=0.95,
            avg_error_rate=0.01,
            avg_latency_ms=110.0,
            most_critical="ok",
        )
    ]
    return Digest(
        generated_at="2024-01-01T00:00:00",
        total_runs=10,
        ok_count=8,
        warning_count=1,
        critical_count=1,
        pipelines=pipelines,
    )


def test_export_statuses_json_is_valid(sample_statuses):
    result = export_statuses(sample_statuses, "json")
    parsed = json.loads(result)
    assert len(parsed) == 3
    assert parsed[0]["pipeline"] == "pipe_a"
    assert parsed[1]["level"] == "warning"
    assert parsed[2]["error_rate"] == pytest.approx(0.25)


def test_export_statuses_csv_has_header(sample_statuses):
    result = export_statuses(sample_statuses, "csv")
    reader = csv.DictReader(io.StringIO(result))
    rows = list(reader)
    assert len(rows) == 3
    assert "pipeline" in reader.fieldnames
    assert rows[0]["pipeline"] == "pipe_a"


def test_export_statuses_text_contains_names(sample_statuses):
    result = export_statuses(sample_statuses, "text")
    assert "pipe_a" in result
    assert "pipe_b" in result
    assert "pipe_c" in result


def test_export_statuses_text_contains_level(sample_statuses):
    result = export_statuses(sample_statuses, "text")
    assert "warning" in result
    assert "critical" in result


def test_export_statuses_invalid_format_raises(sample_statuses):
    with pytest.raises(ValueError, match="Unsupported export format"):
        export_statuses(sample_statuses, "xml")  # type: ignore[arg-type]


def test_export_digest_json_structure(sample_digest):
    result = export_digest(sample_digest, "json")
    parsed = json.loads(result)
    assert parsed["total_runs"] == 10
    assert parsed["ok_count"] == 8
    assert len(parsed["pipelines"]) == 1
    assert parsed["pipelines"][0]["pipeline"] == "pipe_a"


def test_export_digest_text_contains_pipeline(sample_digest):
    result = export_digest(sample_digest, "text")
    assert "pipe_a" in result
    assert "total_runs=10" in result


def test_export_digest_invalid_format_raises(sample_digest):
    with pytest.raises(ValueError, match="Unsupported export format"):
        export_digest(sample_digest, "csv")  # type: ignore[arg-type]
