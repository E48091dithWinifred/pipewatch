"""Tests for pipewatch.silencer."""

from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.silencer import (
    SilenceRule,
    SilencerConfig,
    apply_silencer,
    is_silenced,
)


def _make_status(name: str, level: AlertLevel, error_rate: float = 0.0) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message=f"{name} status",
        error_rate=error_rate,
        latency_ms=100.0,
    )


def _future(hours: int = 2) -> str:
    dt = datetime.now(tz=timezone.utc) + timedelta(hours=hours)
    return dt.isoformat()


def _past(hours: int = 2) -> str:
    dt = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    return dt.isoformat()


# --- SilenceRule.is_active ---

def test_rule_no_expiry_is_always_active():
    rule = SilenceRule(pipeline_name="etl", reason="maintenance")
    assert rule.is_active() is True


def test_rule_future_expiry_is_active():
    rule = SilenceRule(pipeline_name="etl", reason="deploy", until=_future())
    assert rule.is_active() is True


def test_rule_past_expiry_is_not_active():
    rule = SilenceRule(pipeline_name="etl", reason="old", until=_past())
    assert rule.is_active() is False


def test_rule_invalid_until_is_not_active():
    rule = SilenceRule(pipeline_name="etl", reason="bad", until="not-a-date")
    assert rule.is_active() is False


# --- SilenceRule.matches ---

def test_rule_matches_correct_pipeline():
    rule = SilenceRule(pipeline_name="etl", reason="maint")
    status = _make_status("etl", AlertLevel.WARNING)
    assert rule.matches(status) is True


def test_rule_does_not_match_different_pipeline():
    rule = SilenceRule(pipeline_name="etl", reason="maint")
    status = _make_status("other", AlertLevel.WARNING)
    assert rule.matches(status) is False


def test_wildcard_pipeline_matches_any():
    rule = SilenceRule(pipeline_name="*", reason="global")
    assert rule.matches(_make_status("a", AlertLevel.CRITICAL)) is True
    assert rule.matches(_make_status("b", AlertLevel.OK)) is True


def test_rule_filters_by_level_match():
    rule = SilenceRule(pipeline_name="etl", reason="x", levels=["warning"])
    assert rule.matches(_make_status("etl", AlertLevel.WARNING)) is True
    assert rule.matches(_make_status("etl", AlertLevel.CRITICAL)) is False


def test_expired_rule_does_not_match():
    rule = SilenceRule(pipeline_name="etl", reason="old", until=_past())
    assert rule.matches(_make_status("etl", AlertLevel.WARNING)) is False


# --- is_silenced / apply_silencer ---

def test_is_silenced_true_when_rule_matches():
    cfg = SilencerConfig(rules=[SilenceRule(pipeline_name="etl", reason="maint")])
    assert is_silenced(_make_status("etl", AlertLevel.WARNING), cfg) is True


def test_is_silenced_false_when_no_rules():
    cfg = SilencerConfig()
    assert is_silenced(_make_status("etl", AlertLevel.CRITICAL), cfg) is False


def test_apply_silencer_splits_correctly():
    cfg = SilencerConfig(rules=[SilenceRule(pipeline_name="etl", reason="maint")])
    statuses = [
        _make_status("etl", AlertLevel.WARNING),
        _make_status("loader", AlertLevel.OK),
    ]
    active, silenced = apply_silencer(statuses, cfg)
    assert len(active) == 1
    assert active[0].pipeline_name == "loader"
    assert len(silenced) == 1
    assert silenced[0].pipeline_name == "etl"


def test_apply_silencer_all_active_when_no_rules():
    cfg = SilencerConfig()
    statuses = [_make_status("a", AlertLevel.OK), _make_status("b", AlertLevel.CRITICAL)]
    active, silenced = apply_silencer(statuses, cfg)
    assert len(active) == 2
    assert len(silenced) == 0
