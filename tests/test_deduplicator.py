"""Tests for pipewatch.deduplicator."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Optional

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.deduplicator import 
    DedupeEntry,
    filter_statuses,
    record_status,
    should_suppress,
)


def _make_status(name: str = "pipe", level: AlertLevel = AlertLevel.WARNING, msg: str = "") -> PipelineStatus:
    return PipelineStatus(pipeline=name, level=level, message=msg)


@pytest.fixture
def cfg(tmp_path):
    return DedupeConfig(state_path=str(tmp_path / "dedupe.json"), min_repeat=3)


# --- should_suppress ---

def test_suppress_first_occurrence(cfg):
    s = _make_status(level=AlertLevel.WARNING)
    assert should_suppress(s, cfg) is True


def test_suppress_ok_never_suppressed(cfg):
    s = _make_status(level=AlertLevel.OK)
    assert should_suppress(s, cfg) is False


def test_suppress_below_threshold(cfg):
    s = _make_status(level=AlertLevel.CRITICAL)
    record_status(s, cfg, "t1")
    record_status(s, cfg, "t2")
    # count == 2, threshold == 3 → still suppress
    assert should_suppress(s, cfg) is True


def test_no_suppress_at_threshold(cfg):
    s = _make_status(level=AlertLevel.CRITICAL)
    for i in range(3):
        record_status(s, cfg, f"t{i}")
    assert should_suppress(s, cfg) is False


# --- record_status ---

def test_record_increments_count(cfg):
    s = _make_status(level=AlertLevel.WARNING)
    e1 = record_status(s, cfg, "t1")
    e2 = record_status(s, cfg, "t2")
    assert e1.count == 1
    assert e2.count == 2


def test_record_ok_clears_state(cfg):
    warn = _make_status(level=AlertLevel.WARNING)
    record_status(warn, cfg, "t1")
    ok = _make_status(level=AlertLevel.OK)
    entry = record_status(ok, cfg, "t2")
    assert entry.count == 0
    # file should not contain the warning key anymore
    with open(cfg.state_path) as fh:
        state = json.load(fh)
    assert len(state) == 0


def test_record_creates_file(cfg):
    s = _make_status(level=AlertLevel.WARNING)
    record_status(s, cfg, "t1")
    assert os.path.exists(cfg.state_path)


def test_record_stores_last_seen(cfg):
    s = _make_status(level=AlertLevel.WARNING)
    entry = record_status(s, cfg, "2024-01-01T00:00:00")
    assert entry.last_seen == "2024-01-01T00:00:00"


# --- filter_statuses ---

def test_filter_suppresses_below_threshold(cfg):
    statuses = [_make_status("a", AlertLevel.WARNING)] * 2
    result = filter_statuses(statuses, cfg, "t1")
    # after 2 records, count < 3 → suppressed
    assert result == []


def test_filter_passes_at_threshold(cfg):
    s = _make_status("b", AlertLevel.CRITICAL)
    # pre-seed state to count=2
    record_status(s, cfg, "t0")
    record_status(s, cfg, "t1")
    result = filter_statuses([s], cfg, "t2")
    assert len(result) == 1
    assert result[0].pipeline == "b"


def test_filter_ok_always_passes(cfg):
    statuses = [_make_status("c", AlertLevel.OK)]
    result = filter_statuses(statuses, cfg, "t1")
    assert len(result) == 1
