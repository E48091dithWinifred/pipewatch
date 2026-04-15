"""Tests for pipewatch.capper."""
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.capper import CapConfig, CapResult, cap_statuses


def _make_status(name: str, level: AlertLevel) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message=f"{level.value} — {name}",
        error_rate=0.0,
        latency_ms=100.0,
    )


@pytest.fixture()
def sample_statuses():
    return [
        _make_status("pipe-ok-1", AlertLevel.OK),
        _make_status("pipe-warn-1", AlertLevel.WARNING),
        _make_status("pipe-crit-1", AlertLevel.CRITICAL),
        _make_status("pipe-ok-2", AlertLevel.OK),
        _make_status("pipe-warn-2", AlertLevel.WARNING),
        _make_status("pipe-crit-2", AlertLevel.CRITICAL),
    ]


def test_cap_config_default_max():
    cfg = CapConfig()
    assert cfg.max_results == 50


def test_cap_config_invalid_raises():
    with pytest.raises(ValueError):
        CapConfig(max_results=0)


def test_cap_not_applied_when_under_limit(sample_statuses):
    cfg = CapConfig(max_results=10)
    result = cap_statuses(sample_statuses, cfg)
    assert result.cap_applied is False
    assert result.kept_count == len(sample_statuses)
    assert result.dropped_count == 0


def test_cap_applied_when_over_limit(sample_statuses):
    cfg = CapConfig(max_results=3)
    result = cap_statuses(sample_statuses, cfg)
    assert result.cap_applied is True
    assert result.kept_count == 3
    assert result.dropped_count == 3


def test_cap_prefers_critical_first(sample_statuses):
    cfg = CapConfig(max_results=2, prefer_critical=True)
    result = cap_statuses(sample_statuses, cfg)
    levels = [s.level for s in result.kept]
    assert all(lvl == AlertLevel.CRITICAL for lvl in levels)


def test_cap_no_prefer_critical_keeps_original_order(sample_statuses):
    cfg = CapConfig(max_results=2, prefer_critical=False)
    result = cap_statuses(sample_statuses, cfg)
    assert result.kept == sample_statuses[:2]


def test_cap_result_summary_no_cap(sample_statuses):
    cfg = CapConfig(max_results=100)
    result = cap_statuses(sample_statuses, cfg)
    assert "not applied" in result.summary()


def test_cap_result_summary_with_cap(sample_statuses):
    cfg = CapConfig(max_results=2)
    result = cap_statuses(sample_statuses, cfg)
    assert "cap applied" in result.summary()
    assert "2" in result.summary()


def test_cap_empty_list():
    result = cap_statuses([], CapConfig(max_results=5))
    assert result.kept_count == 0
    assert result.cap_applied is False


def test_cap_default_config_used_when_none(sample_statuses):
    result = cap_statuses(sample_statuses)
    # default max is 50, fixture has 6 — no cap
    assert result.cap_applied is False
