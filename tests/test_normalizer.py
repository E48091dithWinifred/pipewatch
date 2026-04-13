"""Tests for pipewatch.normalizer."""

from __future__ import annotations

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.normalizer import NormalizedStatus, normalize_all, normalize_status


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


def test_normalize_status_returns_normalized_instance():
    s = _make_status()
    result = normalize_status(s)
    assert isinstance(result, NormalizedStatus)


def test_normalize_status_name_preserved():
    s = _make_status(name="etl_orders")
    assert normalize_status(s).name == "etl_orders"


def test_normalize_status_level_is_string():
    s = _make_status(level=AlertLevel.WARNING)
    result = normalize_status(s)
    assert isinstance(result.level, str)
    assert result.level == "WARNING"


def test_normalize_status_critical_level():
    s = _make_status(level=AlertLevel.CRITICAL)
    assert normalize_status(s).level == "CRITICAL"


def test_normalize_status_error_rate_rounded():
    s = _make_status(error_rate=0.123456789)
    result = normalize_status(s)
    assert result.error_rate == round(0.123456789, 6)


def test_normalize_status_latency_rounded():
    s = _make_status(latency_ms=99.9999999)
    result = normalize_status(s)
    assert result.latency_ms == round(99.9999999, 3)


def test_normalize_status_default_message_used_when_empty():
    s = _make_status(message="")
    result = normalize_status(s, default_message="no message")
    assert result.message == "no message"


def test_normalize_status_original_message_preserved():
    s = _make_status(message="pipeline ok")
    result = normalize_status(s, default_message="fallback")
    assert result.message == "pipeline ok"


def test_normalize_status_extra_tags_attached():
    s = _make_status()
    result = normalize_status(s, extra_tags=["prod", "critical-path"])
    assert "prod" in result.tags
    assert "critical-path" in result.tags


def test_normalize_status_no_tags_by_default():
    s = _make_status()
    assert normalize_status(s).tags == []


def test_is_healthy_true_for_ok():
    s = _make_status(level=AlertLevel.OK)
    assert normalize_status(s).is_healthy is True


def test_is_healthy_false_for_warning():
    s = _make_status(level=AlertLevel.WARNING)
    assert normalize_status(s).is_healthy is False


def test_as_dict_contains_all_keys():
    s = _make_status()
    d = normalize_status(s).as_dict()
    assert set(d.keys()) == {"name", "level", "error_rate", "latency_ms", "message", "tags"}


def test_normalize_all_returns_correct_count():
    statuses = [_make_status(name=f"pipe_{i}") for i in range(4)]
    results = normalize_all(statuses)
    assert len(results) == 4


def test_normalize_all_empty_list():
    assert normalize_all([]) == []


def test_normalize_all_extra_tags_applied_to_all():
    statuses = [_make_status(name=f"p{i}") for i in range(3)]
    results = normalize_all(statuses, extra_tags=["batch"])
    assert all("batch" in r.tags for r in results)
