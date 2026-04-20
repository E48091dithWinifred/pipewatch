"""Tests for pipewatch.truncator."""
from __future__ import annotations

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.truncator import TruncateConfig, TruncateResult, truncate_statuses


def _make_status(name: str, level: AlertLevel = AlertLevel.OK) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        error_rate=0.0,
        latency_ms=100.0,
        message="ok",
    )


@pytest.fixture()
def sample_statuses():
    return [_make_status(f"pipe_{i}") for i in range(8)]


# --- TruncateConfig ---

def test_truncate_config_default_max():
    cfg = TruncateConfig()
    assert cfg.max_items == 10


def test_truncate_config_invalid_raises():
    with pytest.raises(ValueError):
        TruncateConfig(max_items=0)


def test_truncate_config_negative_raises():
    with pytest.raises(ValueError):
        TruncateConfig(max_items=-5)


# --- truncate_statuses ---

def test_truncate_no_config_keeps_all_when_under_default(sample_statuses):
    result = truncate_statuses(sample_statuses)
    assert result.kept_count == len(sample_statuses)


def test_truncate_drops_excess(sample_statuses):
    cfg = TruncateConfig(max_items=3)
    result = truncate_statuses(sample_statuses, cfg)
    assert result.kept_count == 3
    assert result.dropped_count == len(sample_statuses) - 3


def test_truncate_returns_first_items(sample_statuses):
    cfg = TruncateConfig(max_items=2)
    result = truncate_statuses(sample_statuses, cfg)
    assert result.items[0].pipeline_name == "pipe_0"
    assert result.items[1].pipeline_name == "pipe_1"


def test_truncate_empty_input():
    result = truncate_statuses([], TruncateConfig(max_items=5))
    assert result.kept_count == 0
    assert result.dropped_count == 0
    assert not result.was_truncated


def test_truncate_was_truncated_true_when_dropped(sample_statuses):
    cfg = TruncateConfig(max_items=2)
    result = truncate_statuses(sample_statuses, cfg)
    assert result.was_truncated is True


def test_truncate_was_truncated_false_when_all_kept(sample_statuses):
    cfg = TruncateConfig(max_items=100)
    result = truncate_statuses(sample_statuses, cfg)
    assert result.was_truncated is False


def test_truncate_summary_contains_counts(sample_statuses):
    cfg = TruncateConfig(max_items=3)
    result = truncate_statuses(sample_statuses, cfg)
    s = result.summary()
    assert "3" in s
    assert str(len(sample_statuses)) in s


def test_truncate_summary_no_truncation_message(sample_statuses):
    cfg = TruncateConfig(max_items=100)
    result = truncate_statuses(sample_statuses, cfg)
    assert "no truncation" in result.summary()


def test_truncate_result_total_input(sample_statuses):
    cfg = TruncateConfig(max_items=4)
    result = truncate_statuses(sample_statuses, cfg)
    assert result.total_input == len(sample_statuses)
