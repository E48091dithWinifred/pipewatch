"""Tests for pipewatch.auditor."""
import json
import os
import pytest

from pipewatch.auditor import (
    AuditConfig,
    AuditEntry,
    record_event,
    load_audit_log,
    filter_audit_log,
)


@pytest.fixture
def audit_file(tmp_path):
    return str(tmp_path / "audit" / "audit.json")


@pytest.fixture
def cfg(audit_file):
    return AuditConfig(path=audit_file, max_entries=100)


def test_record_event_creates_file(cfg, audit_file):
    record_event(cfg, "pipe_a", "WARNING", "high error rate")
    assert os.path.exists(audit_file)


def test_record_event_returns_audit_entry(cfg):
    entry = record_event(cfg, "pipe_a", "CRITICAL", "latency spike")
    assert isinstance(entry, AuditEntry)
    assert entry.pipeline == "pipe_a"
    assert entry.level == "CRITICAL"
    assert entry.message == "latency spike"


def test_record_event_persists_fields(cfg, audit_file):
    record_event(cfg, "pipe_b", "OK", "recovered", source="checker")
    with open(audit_file) as fh:
        data = json.load(fh)
    assert len(data) == 1
    assert data[0]["pipeline"] == "pipe_b"
    assert data[0]["source"] == "checker"


def test_record_event_appends_multiple(cfg):
    record_event(cfg, "pipe_a", "WARNING", "msg1")
    record_event(cfg, "pipe_b", "CRITICAL", "msg2")
    entries = load_audit_log(cfg)
    assert len(entries) == 2


def test_record_event_trims_to_max_entries(audit_file):
    cfg = AuditConfig(path=audit_file, max_entries=3)
    for i in range(5):
        record_event(cfg, f"pipe_{i}", "WARNING", f"msg {i}")
    entries = load_audit_log(cfg)
    assert len(entries) == 3
    assert entries[-1].pipeline == "pipe_4"


def test_load_audit_log_missing_returns_empty(cfg):
    entries = load_audit_log(cfg)
    assert entries == []


def test_load_audit_log_returns_audit_entry_instances(cfg):
    record_event(cfg, "pipe_x", "OK", "all good")
    entries = load_audit_log(cfg)
    assert all(isinstance(e, AuditEntry) for e in entries)


def test_filter_by_pipeline(cfg):
    record_event(cfg, "pipe_a", "WARNING", "warn")
    record_event(cfg, "pipe_b", "CRITICAL", "crit")
    entries = load_audit_log(cfg)
    filtered = filter_audit_log(entries, pipeline="pipe_a")
    assert len(filtered) == 1
    assert filtered[0].pipeline == "pipe_a"


def test_filter_by_level(cfg):
    record_event(cfg, "pipe_a", "WARNING", "warn")
    record_event(cfg, "pipe_b", "CRITICAL", "crit")
    record_event(cfg, "pipe_c", "WARNING", "warn2")
    entries = load_audit_log(cfg)
    filtered = filter_audit_log(entries, level="WARNING")
    assert len(filtered) == 2


def test_filter_by_pipeline_and_level(cfg):
    record_event(cfg, "pipe_a", "WARNING", "w")
    record_event(cfg, "pipe_a", "CRITICAL", "c")
    record_event(cfg, "pipe_b", "WARNING", "w2")
    entries = load_audit_log(cfg)
    filtered = filter_audit_log(entries, pipeline="pipe_a", level="WARNING")
    assert len(filtered) == 1
    assert filtered[0].level == "WARNING"


def test_entry_timestamp_is_set(cfg):
    entry = record_event(cfg, "pipe_a", "OK", "fine")
    assert entry.timestamp
    assert "T" in entry.timestamp
