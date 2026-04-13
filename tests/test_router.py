"""Tests for pipewatch.router."""
from __future__ import annotations

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.router import (
    RouteRule,
    RouterConfig,
    dispatch_channels,
    route_statuses,
)


def _make_status(
    name: str = "pipe",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.0,
) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message="ok",
        error_rate=error_rate,
        latency_ms=100.0,
    )


def test_route_statuses_default_channel_when_no_rules():
    config = RouterConfig(rules=[], default_channel="fallback")
    statuses = [_make_status("a"), _make_status("b")]
    table = route_statuses(statuses, config)
    assert set(table.keys()) == {"fallback"}
    assert len(table["fallback"]) == 2


def test_route_statuses_by_level():
    config = RouterConfig(
        rules=[
            RouteRule(channel="alerts", levels=["WARNING", "CRITICAL"]),
        ],
        default_channel="ok_channel",
    )
    statuses = [
        _make_status("a", AlertLevel.OK),
        _make_status("b", AlertLevel.WARNING),
        _make_status("c", AlertLevel.CRITICAL),
    ]
    table = route_statuses(statuses, config)
    assert len(table.get("alerts", [])) == 2
    assert len(table.get("ok_channel", [])) == 1


def test_route_statuses_by_name_prefix():
    config = RouterConfig(
        rules=[
            RouteRule(channel="ingestion", name_prefix="ingest_"),
        ],
        default_channel="other",
    )
    statuses = [
        _make_status("ingest_orders"),
        _make_status("ingest_users"),
        _make_status("transform_orders"),
    ]
    table = route_statuses(statuses, config)
    assert len(table["ingestion"]) == 2
    assert len(table["other"]) == 1


def test_route_statuses_first_matching_rule_wins():
    config = RouterConfig(
        rules=[
            RouteRule(channel="critical_only", levels=["CRITICAL"]),
            RouteRule(channel="any_alert", levels=["WARNING", "CRITICAL"]),
        ],
        default_channel="default",
    )
    critical_status = _make_status("x", AlertLevel.CRITICAL)
    table = route_statuses([critical_status], config)
    assert "critical_only" in table
    assert "any_alert" not in table


def test_route_statuses_empty_input_returns_empty_table():
    config = RouterConfig(default_channel="default")
    table = route_statuses([], config)
    assert table == {}


def test_dispatch_channels_calls_handler_per_channel():
    table = {
        "alerts": [_make_status("a", AlertLevel.WARNING)],
        "default": [_make_status("b")],
    }
    calls: list = []
    dispatch_channels(table, lambda ch, ss: calls.append((ch, ss)))
    channels_called = {c for c, _ in calls}
    assert channels_called == {"alerts", "default"}


def test_dispatch_channels_passes_correct_statuses():
    s = _make_status("pipe", AlertLevel.CRITICAL)
    table = {"critical": [s]}
    received = {}
    dispatch_channels(table, lambda ch, ss: received.update({ch: ss}))
    assert received["critical"] == [s]
