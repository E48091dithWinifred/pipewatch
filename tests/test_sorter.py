"""Tests for pipewatch.sorter."""
import pytest
from pipewatch.checker import PipelineStatus, AlertLevel
from pipewatch.sorter import SortConfig, SortResult, sort_statuses


def _make_status(name: str, level: AlertLevel, error_rate: float = 0.0, latency: float = 0.0) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message="",
        error_rate=error_rate,
        latency_ms=latency,
    )


@pytest.fixture
def sample_statuses():
    return [
        _make_status("beta", AlertLevel.OK, error_rate=0.05, latency=200.0),
        _make_status("alpha", AlertLevel.CRITICAL, error_rate=0.30, latency=900.0),
        _make_status("gamma", AlertLevel.WARNING, error_rate=0.12, latency=500.0),
    ]


def test_sort_config_defaults():
    cfg = SortConfig()
    assert cfg.key == "level"
    assert cfg.reverse is False


def test_sort_config_invalid_key_raises():
    with pytest.raises(ValueError, match="sort key"):
        SortConfig(key="unknown")


def test_sort_by_level_worst_first(sample_statuses):
    result = sort_statuses(sample_statuses, SortConfig(key="level"))
    assert result.items[0].level == AlertLevel.CRITICAL
    assert result.items[-1].level == AlertLevel.OK


def test_sort_by_level_best_first(sample_statuses):
    result = sort_statuses(sample_statuses, SortConfig(key="level", reverse=True))
    assert result.items[0].level == AlertLevel.OK


def test_sort_by_error_rate_ascending(sample_statuses):
    result = sort_statuses(sample_statuses, SortConfig(key="error_rate"))
    rates = [s.error_rate for s in result.items]
    assert rates == sorted(rates)


def test_sort_by_latency_descending(sample_statuses):
    result = sort_statuses(sample_statuses, SortConfig(key="latency", reverse=True))
    latencies = [s.latency_ms for s in result.items]
    assert latencies == sorted(latencies, reverse=True)


def test_sort_by_name_alphabetical(sample_statuses):
    result = sort_statuses(sample_statuses, SortConfig(key="name"))
    names = [s.pipeline_name for s in result.items]
    assert names == ["alpha", "beta", "gamma"]


def test_sort_empty_returns_empty():
    result = sort_statuses([], SortConfig(key="name"))
    assert result.items == []
    assert result.count == 0


def test_sort_result_count(sample_statuses):
    result = sort_statuses(sample_statuses)
    assert result.count == 3


def test_sort_result_summary_contains_key(sample_statuses):
    result = sort_statuses(sample_statuses, SortConfig(key="error_rate"))
    assert "error_rate" in result.summary()


def test_sort_result_summary_contains_direction(sample_statuses):
    result = sort_statuses(sample_statuses, SortConfig(key="name", reverse=True))
    assert "desc" in result.summary()


def test_sort_default_config_used_when_none(sample_statuses):
    result = sort_statuses(sample_statuses)
    assert result.key == "level"
