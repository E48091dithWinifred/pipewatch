"""Tests for pipewatch.formatter."""
import json
import csv
import io

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.formatter import FormatterConfig, format_table, format_json, format_csv, render


def _make_status(
    name: str = "pipe_a",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.01,
    latency_ms: float = 120.5,
    message: str = "all good",
) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        error_rate=error_rate,
        latency_ms=latency_ms,
        message=message,
    )


@pytest.fixture()
def sample_statuses():
    return [
        _make_status("alpha", AlertLevel.OK, 0.01, 100.0, "ok"),
        _make_status("beta", AlertLevel.WARNING, 0.06, 250.0, "slow"),
        _make_status("gamma", AlertLevel.CRITICAL, 0.20, 800.0, "high error rate"),
    ]


# --- table ---

def test_format_table_contains_header(sample_statuses):
    out = format_table(sample_statuses, FormatterConfig())
    assert "PIPELINE" in out
    assert "LEVEL" in out


def test_format_table_contains_pipeline_names(sample_statuses):
    out = format_table(sample_statuses, FormatterConfig())
    assert "alpha" in out
    assert "beta" in out
    assert "gamma" in out


def test_format_table_contains_levels(sample_statuses):
    out = format_table(sample_statuses, FormatterConfig())
    assert "ok" in out.lower()
    assert "warning" in out.lower()
    assert "critical" in out.lower()


def test_format_table_max_rows(sample_statuses):
    cfg = FormatterConfig(max_rows=2)
    out = format_table(sample_statuses, cfg)
    assert "gamma" not in out
    assert "Total: 2" in out


def test_format_table_hide_message(sample_statuses):
    cfg = FormatterConfig(show_message=False)
    out = format_table(sample_statuses, cfg)
    # messages should not appear
    assert "high error rate" not in out


# --- json ---

def test_format_json_is_valid_json(sample_statuses):
    out = format_json(sample_statuses, FormatterConfig())
    parsed = json.loads(out)
    assert isinstance(parsed, list)
    assert len(parsed) == 3


def test_format_json_fields(sample_statuses):
    out = format_json(sample_statuses, FormatterConfig())
    parsed = json.loads(out)
    first = parsed[0]
    assert "pipeline_name" in first
    assert "level" in first
    assert "error_rate" in first
    assert "latency_ms" in first
    assert "message" in first


def test_format_json_hide_message(sample_statuses):
    cfg = FormatterConfig(show_message=False)
    out = format_json(sample_statuses, cfg)
    parsed = json.loads(out)
    assert parsed[0]["message"] is None


# --- csv ---

def test_format_csv_has_header(sample_statuses):
    out = format_csv(sample_statuses, FormatterConfig())
    reader = csv.DictReader(io.StringIO(out))
    assert "pipeline_name" in reader.fieldnames
    assert "level" in reader.fieldnames


def test_format_csv_row_count(sample_statuses):
    out = format_csv(sample_statuses, FormatterConfig())
    rows = list(csv.DictReader(io.StringIO(out)))
    assert len(rows) == 3


def test_format_csv_values(sample_statuses):
    out = format_csv(sample_statuses, FormatterConfig())
    rows = list(csv.DictReader(io.StringIO(out)))
    assert rows[1]["pipeline_name"] == "beta"
    assert rows[1]["level"] == "warning"


# --- render dispatch ---

def test_render_default_is_table(sample_statuses):
    out = render(sample_statuses)
    assert "PIPELINE" in out


def test_render_json_format(sample_statuses):
    cfg = FormatterConfig(fmt="json")
    out = render(sample_statuses, cfg)
    assert json.loads(out) is not None


def test_render_csv_format(sample_statuses):
    cfg = FormatterConfig(fmt="csv")
    out = render(sample_statuses, cfg)
    assert "pipeline_name" in out
