"""Tests for pipewatch.alerts."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.alerts import (
    AlertConfig,
    AlertEvent,
    should_fire,
    build_alert_event,
    send_email_alert,
    dispatch_alerts,
)

TIMESTAMP = "2024-01-15T10:00:00"


def _make_status(level: AlertLevel, name: str = "pipe", msg: str = "test msg"):
    return PipelineStatus(pipeline_name=name, level=level, message=msg)


# --- should_fire ---

def test_should_fire_ok_below_warning():
    status = _make_status(AlertLevel.OK)
    assert should_fire(status, AlertLevel.WARNING) is False


def test_should_fire_warning_meets_warning():
    status = _make_status(AlertLevel.WARNING)
    assert should_fire(status, AlertLevel.WARNING) is True


def test_should_fire_critical_meets_warning():
    status = _make_status(AlertLevel.CRITICAL)
    assert should_fire(status, AlertLevel.WARNING) is True


def test_should_fire_warning_below_critical():
    status = _make_status(AlertLevel.WARNING)
    assert should_fire(status, AlertLevel.CRITICAL) is False


def test_should_fire_critical_meets_critical():
    status = _make_status(AlertLevel.CRITICAL)
    assert should_fire(status, AlertLevel.CRITICAL) is True


# --- build_alert_event ---

def test_build_alert_event_fields():
    status = _make_status(AlertLevel.WARNING, name="orders", msg="high latency")
    event = build_alert_event(status, TIMESTAMP)
    assert event.pipeline_name == "orders"
    assert event.level == AlertLevel.WARNING
    assert event.message == "high latency"
    assert event.timestamp == TIMESTAMP


def test_build_alert_event_none_message():
    status = PipelineStatus(pipeline_name="p", level=AlertLevel.OK, message=None)
    event = build_alert_event(status, TIMESTAMP)
    assert event.message == ""


# --- send_email_alert ---

def test_send_email_alert_no_recipients_returns_false():
    event = AlertEvent("p", AlertLevel.WARNING, "msg", TIMESTAMP)
    config = AlertConfig(email_to=[])
    assert send_email_alert(event, config) is False


def test_send_email_alert_calls_smtp(monkeypatch):
    event = AlertEvent("pipe", AlertLevel.CRITICAL, "error rate high", TIMESTAMP)
    config = AlertConfig(email_to=["ops@example.com"], smtp_host="mail", smtp_port=587)

    mock_smtp_instance = MagicMock()
    mock_smtp_cls = MagicMock(return_value=__import__('contextlib').nullcontext(mock_smtp_instance))

    # Use a real context manager mock
    smtp_mock = MagicMock()
    smtp_mock.return_value.__enter__ = MagicMock(return_value=smtp_mock)
    smtp_mock.return_value.__exit__ = MagicMock(return_value=False)

    result = send_email_alert(event, config, smtp_cls=smtp_mock)
    assert result is True
    smtp_mock.assert_called_once_with("mail", 587)


# --- dispatch_alerts ---

def test_dispatch_alerts_returns_only_fired():
    statuses = [
        _make_status(AlertLevel.OK, name="a"),
        _make_status(AlertLevel.WARNING, name="b"),
        _make_status(AlertLevel.CRITICAL, name="c"),
    ]
    config = AlertConfig(email_to=[])  # no email, but events still returned
    fired = dispatch_alerts(statuses, config, TIMESTAMP)
    assert len(fired) == 2
    assert fired[0].pipeline_name == "b"
    assert fired[1].pipeline_name == "c"


def test_dispatch_alerts_empty_statuses():
    config = AlertConfig()
    fired = dispatch_alerts([], config, TIMESTAMP)
    assert fired == []
