"""Tests for pipewatch.notifier."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipewatch.alerts import AlertEvent
from pipewatch.checker import AlertLevel
from pipewatch.notifier import NotifierConfig, NotificationResult, dispatch, _send_log


def _make_event(level: AlertLevel = AlertLevel.WARNING) -> AlertEvent:
    return AlertEvent(
        pipeline_name="orders",
        level=level,
        message="error rate exceeded threshold",
        timestamp="2024-01-01T00:00:00",
    )


# ---------------------------------------------------------------------------
# _send_log
# ---------------------------------------------------------------------------

def test_send_log_returns_success(capsys):
    event = _make_event()
    result = _send_log(event)
    assert result.success is True
    assert result.channel == "log"


def test_send_log_prints_pipeline_name(capsys):
    event = _make_event()
    _send_log(event)
    captured = capsys.readouterr()
    assert "orders" in captured.out


def test_send_log_prints_level(capsys):
    event = _make_event(AlertLevel.CRITICAL)
    _send_log(event)
    captured = capsys.readouterr()
    assert "CRITICAL" in captured.out


# ---------------------------------------------------------------------------
# dispatch — log channel
# ---------------------------------------------------------------------------

def test_dispatch_log_enabled_returns_log_result(capsys):
    cfg = NotifierConfig(log_enabled=True)
    results = dispatch(_make_event(), cfg)
    channels = [r.channel for r in results]
    assert "log" in channels


def test_dispatch_log_disabled_no_log_result(capsys):
    cfg = NotifierConfig(log_enabled=False)
    results = dispatch(_make_event(), cfg)
    channels = [r.channel for r in results]
    assert "log" not in channels


# ---------------------------------------------------------------------------
# dispatch — email channel
# ---------------------------------------------------------------------------

def test_dispatch_email_calls_email_fn():
    mock_email = MagicMock()
    cfg = NotifierConfig(log_enabled=False, email_enabled=True)
    dispatch(_make_event(), cfg, _email_fn=mock_email)
    mock_email.assert_called_once()


def test_dispatch_email_success_result():
    cfg = NotifierConfig(log_enabled=False, email_enabled=True)
    results = dispatch(_make_event(), cfg, _email_fn=lambda e: None)
    email_results = [r for r in results if r.channel == "email"]
    assert len(email_results) == 1
    assert email_results[0].success is True


def test_dispatch_email_failure_captured():
    def bad_email(event):
        raise RuntimeError("SMTP down")

    cfg = NotifierConfig(log_enabled=False, email_enabled=True)
    results = dispatch(_make_event(), cfg, _email_fn=bad_email)
    email_results = [r for r in results if r.channel == "email"]
    assert email_results[0].success is False
    assert "SMTP down" in email_results[0].message


# ---------------------------------------------------------------------------
# dispatch — webhook channel
# ---------------------------------------------------------------------------

def test_dispatch_webhook_disabled_no_webhook_result():
    cfg = NotifierConfig(log_enabled=False, webhook_enabled=False, webhook_url="http://x")
    results = dispatch(_make_event(), cfg)
    assert all(r.channel != "webhook" for r in results)


def test_dispatch_webhook_no_url_no_webhook_result():
    cfg = NotifierConfig(log_enabled=False, webhook_enabled=True, webhook_url=None)
    results = dispatch(_make_event(), cfg)
    assert all(r.channel != "webhook" for r in results)


# ---------------------------------------------------------------------------
# dispatch — combined
# ---------------------------------------------------------------------------

def test_dispatch_multiple_channels_returns_multiple_results():
    cfg = NotifierConfig(log_enabled=True, email_enabled=True)
    results = dispatch(_make_event(), cfg, _email_fn=lambda e: None)
    assert len(results) == 2
