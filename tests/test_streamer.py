"""Tests for pipewatch.streamer."""
from __future__ import annotations

from typing import List

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.streamer import StreamConfig, StreamResult, batch_stream, stream_statuses


def _make_status(name: str, level: str = "ok", error_rate: float = 0.0) -> PipelineStatus:
    lvl = AlertLevel[level.upper()]
    return PipelineStatus(
        pipeline_name=name,
        level=lvl,
        error_rate=error_rate,
        latency_ms=100.0,
        message=f"{level} status",
    )


@pytest.fixture()
def sample_statuses() -> List[PipelineStatus]:
    return [
        _make_status("alpha", "ok"),
        _make_status("beta", "warning", 0.05),
        _make_status("gamma", "critical", 0.25),
        _make_status("delta", "ok"),
        _make_status("epsilon", "warning", 0.08),
    ]


def test_stream_result_total_seen():
    r = StreamResult(emitted=[_make_status("a")], dropped=3)
    assert r.total_seen == 4


def test_stream_result_summary_contains_counts():
    r = StreamResult(emitted=[_make_status("a"), _make_status("b")], dropped=1)
    s = r.summary()
    assert "emitted=2" in s
    assert "dropped=1" in s
    assert "total=3" in s


def test_stream_statuses_no_config_emits_all(sample_statuses):
    result = stream_statuses(sample_statuses)
    assert len(result.emitted) == 5
    assert result.dropped == 0


def test_stream_statuses_drop_ok_filters_ok(sample_statuses):
    cfg = StreamConfig(drop_ok=True)
    result = stream_statuses(sample_statuses, cfg=cfg)
    assert all(s.level != AlertLevel.OK for s in result.emitted)
    assert result.dropped == 2


def test_stream_statuses_max_items_limits_emitted(sample_statuses):
    cfg = StreamConfig(max_items=3)
    result = stream_statuses(sample_statuses, cfg=cfg)
    assert len(result.emitted) == 3
    assert result.dropped == 2


def test_stream_statuses_on_emit_callback_called(sample_statuses):
    seen: List[str] = []
    stream_statuses(sample_statuses, on_emit=lambda s: seen.append(s.pipeline_name))
    assert seen == ["alpha", "beta", "gamma", "delta", "epsilon"]


def test_stream_statuses_drop_ok_and_max_items_combined(sample_statuses):
    cfg = StreamConfig(drop_ok=True, max_items=2)
    result = stream_statuses(sample_statuses, cfg=cfg)
    assert len(result.emitted) == 2
    assert result.total_seen == 5


def test_batch_stream_even_split(sample_statuses):
    batches = list(batch_stream(sample_statuses, batch_size=2))
    assert len(batches) == 3
    assert len(batches[0]) == 2
    assert len(batches[1]) == 2
    assert len(batches[2]) == 1


def test_batch_stream_single_batch_when_small(sample_statuses):
    batches = list(batch_stream(sample_statuses, batch_size=10))
    assert len(batches) == 1
    assert len(batches[0]) == 5


def test_batch_stream_empty_source():
    batches = list(batch_stream([], batch_size=5))
    assert batches == []


def test_stream_statuses_empty_source():
    result = stream_statuses([])
    assert result.emitted == []
    assert result.dropped == 0
    assert result.total_seen == 0
