"""Tests for pipewatch.archiver."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.archiver import ArchiveConfig, ArchiveResult, archive_old_records


def _iso(days_ago: int) -> str:
    dt = datetime.now(tz=timezone.utc) - timedelta(days=days_ago)
    return dt.isoformat()


def _write(path: Path, records: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records))


@pytest.fixture()
def tmp_paths(tmp_path):
    history = tmp_path / "history.json"
    archive = tmp_path / "archive.json"
    return history, archive


def test_archive_result_summary():
    r = ArchiveResult(archived=3, pruned=3, kept=7, archive_path="/tmp/a.json")
    assert "archived=3" in r.summary
    assert "kept=7" in r.summary


def test_archive_empty_history(tmp_paths):
    history, archive = tmp_paths
    cfg = ArchiveConfig(str(history), str(archive), max_age_days=30)
    result = archive_old_records(cfg)
    assert result.archived == 0
    assert result.kept == 0


def test_archive_moves_old_records(tmp_paths):
    history, archive = tmp_paths
    records = [
        {"pipeline": "a", "timestamp": _iso(40), "level": "OK"},
        {"pipeline": "b", "timestamp": _iso(10), "level": "OK"},
    ]
    _write(history, records)
    cfg = ArchiveConfig(str(history), str(archive), max_age_days=30)
    result = archive_old_records(cfg)

    assert result.archived == 1
    assert result.kept == 1


def test_archive_writes_to_archive_file(tmp_paths):
    history, archive = tmp_paths
    records = [{"pipeline": "old", "timestamp": _iso(60), "level": "CRITICAL"}]
    _write(history, records)
    cfg = ArchiveConfig(str(history), str(archive), max_age_days=30)
    archive_old_records(cfg)

    archived = json.loads(archive.read_text())
    assert len(archived) == 1
    assert archived[0]["pipeline"] == "old"


def test_archive_removes_from_history(tmp_paths):
    history, archive = tmp_paths
    records = [
        {"pipeline": "old", "timestamp": _iso(50), "level": "OK"},
        {"pipeline": "new", "timestamp": _iso(5), "level": "OK"},
    ]
    _write(history, records)
    cfg = ArchiveConfig(str(history), str(archive), max_age_days=30)
    archive_old_records(cfg)

    remaining = json.loads(history.read_text())
    assert len(remaining) == 1
    assert remaining[0]["pipeline"] == "new"


def test_dry_run_does_not_write(tmp_paths):
    history, archive = tmp_paths
    records = [{"pipeline": "old", "timestamp": _iso(60), "level": "OK"}]
    _write(history, records)
    cfg = ArchiveConfig(str(history), str(archive), max_age_days=30, dry_run=True)
    result = archive_old_records(cfg)

    assert result.archived == 1
    assert result.archive_path is None
    assert not archive.exists()
    # history file should be unchanged
    remaining = json.loads(history.read_text())
    assert len(remaining) == 1


def test_archive_appends_to_existing_archive(tmp_paths):
    history, archive = tmp_paths
    existing = [{"pipeline": "previous", "timestamp": _iso(90), "level": "OK"}]
    _write(archive, existing)
    records = [{"pipeline": "old", "timestamp": _iso(45), "level": "WARNING"}]
    _write(history, records)
    cfg = ArchiveConfig(str(history), str(archive), max_age_days=30)
    archive_old_records(cfg)

    archived = json.loads(archive.read_text())
    assert len(archived) == 2


def test_records_with_bad_timestamp_are_kept(tmp_paths):
    history, archive = tmp_paths
    records = [{"pipeline": "x", "timestamp": "not-a-date", "level": "OK"}]
    _write(history, records)
    cfg = ArchiveConfig(str(history), str(archive), max_age_days=30)
    result = archive_old_records(cfg)
    assert result.kept == 1
    assert result.archived == 0
