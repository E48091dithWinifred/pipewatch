"""Tests for pipewatch/merger.py"""
import pytest
from pipewatch.checker import PipelineStatus, AlertLevel
from pipewatch.merger import MergeConfig, MergeResult, merge_statuses


def _make_status(name: str, level: AlertLevel, error_rate: float = 0.0) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        error_rate=error_rate,
        latency_ms=100.0,
        message=level.value,
    )


# --- MergeResult ---

def test_merge_result_total_merged():
    s = [_make_status("a", AlertLevel.OK)]
    result = MergeResult(statuses=s)
    assert result.total_merged == 1


def test_merge_result_summary_contains_count():
    result = MergeResult(
        statuses=[_make_status("a", AlertLevel.OK)],
        source_counts={"source_0": 1},
        duplicate_count=0,
    )
    assert "1" in result.summary()
    assert "source" in result.summary()


# --- merge_statuses ---

def test_merge_empty_sources_returns_empty():
    result = merge_statuses([])
    assert result.statuses == []
    assert result.duplicate_count == 0


def test_merge_single_source_returns_all():
    source = [_make_status("pipe_a", AlertLevel.OK), _make_status("pipe_b", AlertLevel.WARNING)]
    result = merge_statuses([source])
    assert len(result.statuses) == 2


def test_merge_no_duplicates_across_sources():
    a = [_make_status("pipe_a", AlertLevel.OK)]
    b = [_make_status("pipe_b", AlertLevel.WARNING)]
    result = merge_statuses([a, b])
    assert len(result.statuses) == 2
    assert result.duplicate_count == 0


def test_merge_duplicate_latest_strategy_picks_incoming():
    a = [_make_status("pipe_a", AlertLevel.OK)]
    b = [_make_status("pipe_a", AlertLevel.WARNING)]
    result = merge_statuses([a, b], MergeConfig(strategy="latest"))
    assert len(result.statuses) == 1
    assert result.statuses[0].level == AlertLevel.WARNING
    assert result.duplicate_count == 1


def test_merge_duplicate_first_strategy_keeps_existing():
    a = [_make_status("pipe_a", AlertLevel.WARNING)]
    b = [_make_status("pipe_a", AlertLevel.CRITICAL)]
    result = merge_statuses([a, b], MergeConfig(strategy="first"))
    assert result.statuses[0].level == AlertLevel.WARNING


def test_merge_duplicate_worst_strategy_picks_higher_level():
    a = [_make_status("pipe_a", AlertLevel.OK)]
    b = [_make_status("pipe_a", AlertLevel.CRITICAL)]
    result = merge_statuses([a, b], MergeConfig(strategy="worst"))
    assert result.statuses[0].level == AlertLevel.CRITICAL


def test_merge_worst_does_not_downgrade():
    a = [_make_status("pipe_a", AlertLevel.CRITICAL)]
    b = [_make_status("pipe_a", AlertLevel.OK)]
    result = merge_statuses([a, b], MergeConfig(strategy="worst"))
    assert result.statuses[0].level == AlertLevel.CRITICAL


def test_merge_source_counts_recorded():
    a = [_make_status("x", AlertLevel.OK), _make_status("y", AlertLevel.OK)]
    b = [_make_status("z", AlertLevel.OK)]
    result = merge_statuses([a, b])
    assert result.source_counts["source_0"] == 2
    assert result.source_counts["source_1"] == 1


def test_merge_deduplicate_false_keeps_last_occurrence():
    a = [_make_status("pipe_a", AlertLevel.OK)]
    b = [_make_status("pipe_a", AlertLevel.CRITICAL)]
    # deduplicate=False still overwrites (dict key collision) but no duplicate_count bump
    result = merge_statuses([a, b], MergeConfig(deduplicate=False))
    assert result.duplicate_count == 0
