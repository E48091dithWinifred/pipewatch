"""Tests for pipewatch.history persistence module."""

import json
from pathlib import Path

import pytest

from pipewatch.history import (
    RunRecord,
    append_run,
    clear_history,
    load_history,
)


@pytest.fixture()
def history_file(tmp_path: Path) -> Path:
    return tmp_path / "test_history.json"


def _make_record(name: str = "etl_sales", level: str = "OK") -> RunRecord:
    return RunRecord(
        pipeline_name=name,
        timestamp=RunRecord.now_iso(),
        alert_level=level,
        error_rate=0.01,
        latency_ms=120.5,
        rows_per_second=500.0,
        message=None,
    )


def test_append_creates_file(history_file: Path) -> None:
    append_run(_make_record(), path=history_file)
    assert history_file.exists()


def test_append_stores_correct_fields(history_file: Path) -> None:
    record = _make_record(name="etl_orders", level="WARNING")
    append_run(record, path=history_file)
    data = json.loads(history_file.read_text())
    assert len(data) == 1
    assert data[0]["pipeline_name"] == "etl_orders"
    assert data[0]["alert_level"] == "WARNING"


def test_append_multiple_records(history_file: Path) -> None:
    for level in ("OK", "WARNING", "CRITICAL"):
        append_run(_make_record(level=level), path=history_file)
    records = load_history(path=history_file)
    assert len(records) == 3


def test_load_history_filter_by_name(history_file: Path) -> None:
    append_run(_make_record(name="pipe_a"), path=history_file)
    append_run(_make_record(name="pipe_b"), path=history_file)
    append_run(_make_record(name="pipe_a"), path=history_file)
    results = load_history(pipeline_name="pipe_a", path=history_file)
    assert len(results) == 2
    assert all(r.pipeline_name == "pipe_a" for r in results)


def test_load_history_limit(history_file: Path) -> None:
    for _ in range(10):
        append_run(_make_record(), path=history_file)
    results = load_history(path=history_file, limit=3)
    assert len(results) == 3


def test_load_history_empty_file(history_file: Path) -> None:
    results = load_history(path=history_file)
    assert results == []


def test_clear_history_removes_file(history_file: Path) -> None:
    append_run(_make_record(), path=history_file)
    clear_history(path=history_file)
    assert not history_file.exists()


def test_clear_history_noop_when_missing(history_file: Path) -> None:
    # Should not raise even if file does not exist
    clear_history(path=history_file)
