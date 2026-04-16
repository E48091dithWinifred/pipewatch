"""Tests for pipewatch.batcher."""
import pytest
from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.batcher import BatchConfig, Batch, BatchResult, batch_statuses


def _make_status(name: str, level: AlertLevel = AlertLevel.OK) -> PipelineStatus:
    return PipelineStatus(pipeline_name=name, level=level, message="ok", error_rate=0.0, latency_ms=100.0)


def test_batch_config_default_size():
    cfg = BatchConfig()
    assert cfg.size == 10


def test_batch_config_invalid_raises():
    with pytest.raises(ValueError):
        BatchConfig(size=0)


def test_batch_empty_input_returns_no_batches():
    result = batch_statuses([])
    assert result.total_batches == 0
    assert result.total_items == 0


def test_batch_single_item():
    statuses = [_make_status("p1")]
    result = batch_statuses(statuses, BatchConfig(size=5))
    assert result.total_batches == 1
    assert result.total_items == 1


def test_batch_exact_multiple():
    statuses = [_make_status(f"p{i}") for i in range(6)]
    result = batch_statuses(statuses, BatchConfig(size=3))
    assert result.total_batches == 2
    assert all(b.count == 3 for b in result.batches)


def test_batch_remainder_in_last_batch():
    statuses = [_make_status(f"p{i}") for i in range(7)]
    result = batch_statuses(statuses, BatchConfig(size=3))
    assert result.total_batches == 3
    assert result.batches[-1].count == 1


def test_batch_index_starts_at_one():
    statuses = [_make_status(f"p{i}") for i in range(4)]
    result = batch_statuses(statuses, BatchConfig(size=2))
    assert result.batches[0].index == 1
    assert result.batches[1].index == 2


def test_batch_items_preserved():
    statuses = [_make_status("alpha"), _make_status("beta")]
    result = batch_statuses(statuses, BatchConfig(size=10))
    assert result.batches[0].items[0].pipeline_name == "alpha"
    assert result.batches[0].items[1].pipeline_name == "beta"


def test_batch_summary_contains_count():
    b = Batch(index=1, items=[_make_status("x")])
    assert "1" in b.summary


def test_batch_result_summary():
    statuses = [_make_status(f"p{i}") for i in range(5)]
    result = batch_statuses(statuses, BatchConfig(size=2))
    assert "5" in result.summary
    assert "3" in result.summary


def test_batch_uses_default_config_when_none():
    statuses = [_make_status(f"p{i}") for i in range(25)]
    result = batch_statuses(statuses)
    assert result.total_batches == 3
