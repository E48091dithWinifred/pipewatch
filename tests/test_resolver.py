"""Tests for pipewatch.resolver."""
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.resolver import ResolvedAction, resolve_action, resolve_all


def _make_status(
    name="pipe",
    level=AlertLevel.OK,
    error_rate=0.0,
    latency_ms=100.0,
    message="",
) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        error_rate=error_rate,
        latency_ms=latency_ms,
        message=message,
    )


def test_resolve_action_ok_returns_monitor():
    result = resolve_action(_make_status(level=AlertLevel.OK))
    assert result.action == "monitor"
    assert result.priority == 3


def test_resolve_action_warning_returns_investigate():
    result = resolve_action(_make_status(level=AlertLevel.WARNING))
    assert result.action == "investigate"
    assert result.priority == 2


def test_resolve_action_critical_returns_remediate():
    result = resolve_action(_make_status(level=AlertLevel.CRITICAL))
    assert result.action == "remediate immediately"
    assert result.priority == 1


def test_resolve_action_is_urgent_only_for_critical():
    ok = resolve_action(_make_status(level=AlertLevel.OK))
    warn = resolve_action(_make_status(level=AlertLevel.WARNING))
    crit = resolve_action(_make_status(level=AlertLevel.CRITICAL))
    assert not ok.is_urgent
    assert not warn.is_urgent
    assert crit.is_urgent


def test_resolve_action_high_error_rate_adds_note():
    result = resolve_action(_make_status(level=AlertLevel.WARNING, error_rate=0.25))
    assert any("error rate" in n for n in result.notes)


def test_resolve_action_low_error_rate_no_note():
    result = resolve_action(_make_status(level=AlertLevel.WARNING, error_rate=0.05))
    assert not any("error rate" in n for n in result.notes)


def test_resolve_action_high_latency_adds_note():
    result = resolve_action(_make_status(level=AlertLevel.CRITICAL, latency_ms=8000))
    assert any("latency" in n for n in result.notes)


def test_resolve_action_message_added_to_notes():
    result = resolve_action(_make_status(level=AlertLevel.WARNING, message="stale data"))
    assert "stale data" in result.notes


def test_resolve_action_summary_contains_name_and_action():
    result = resolve_action(_make_status(name="etl_orders", level=AlertLevel.CRITICAL))
    s = result.summary()
    assert "etl_orders" in s
    assert "remediate" in s


def test_resolve_all_sorted_by_priority():
    statuses = [
        _make_status("a", AlertLevel.OK),
        _make_status("b", AlertLevel.CRITICAL),
        _make_status("c", AlertLevel.WARNING),
    ]
    results = resolve_all(statuses)
    priorities = [r.priority for r in results]
    assert priorities == sorted(priorities)


def test_resolve_all_min_priority_filters():
    statuses = [
        _make_status("a", AlertLevel.OK),
        _make_status("b", AlertLevel.CRITICAL),
        _make_status("c", AlertLevel.WARNING),
    ]
    results = resolve_all(statuses, min_priority=2)
    assert all(r.priority <= 2 for r in results)
    assert len(results) == 2


def test_resolve_all_empty_returns_empty():
    assert resolve_all([]) == []
