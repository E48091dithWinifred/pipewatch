"""Notification routing: dispatches alert events to configured channels."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional

from pipewatch.alerts import AlertEvent, send_email_alert


@dataclass
class NotifierConfig:
    email_enabled: bool = False
    webhook_enabled: bool = False
    webhook_url: Optional[str] = None
    log_enabled: bool = True


@dataclass
class NotificationResult:
    channel: str
    success: bool
    message: str = ""


def _send_log(event: AlertEvent) -> NotificationResult:
    """Log-based notification (always available)."""
    msg = f"[pipewatch] {event.level.value.upper()} — {event.pipeline_name}: {event.message}"
    print(msg)
    return NotificationResult(channel="log", success=True, message=msg)


def _send_webhook(event: AlertEvent, url: str) -> NotificationResult:
    """POST alert payload to a webhook URL."""
    try:
        import urllib.request, json as _json
        payload = _json.dumps({
            "pipeline": event.pipeline_name,
            "level": event.level.value,
            "message": event.message,
            "timestamp": event.timestamp,
        }).encode()
        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
        return NotificationResult(channel="webhook", success=True)
    except Exception as exc:  # pragma: no cover
        return NotificationResult(channel="webhook", success=False, message=str(exc))


def dispatch(
    event: AlertEvent,
    config: NotifierConfig,
    _email_fn: Callable[[AlertEvent], None] = send_email_alert,
) -> List[NotificationResult]:
    """Dispatch an alert event to all enabled channels."""
    results: List[NotificationResult] = []

    if config.log_enabled:
        results.append(_send_log(event))

    if config.email_enabled:
        try:
            _email_fn(event)
            results.append(NotificationResult(channel="email", success=True))
        except Exception as exc:
            results.append(NotificationResult(channel="email", success=False, message=str(exc)))

    if config.webhook_enabled and config.webhook_url:
        results.append(_send_webhook(event, config.webhook_url))

    return results
