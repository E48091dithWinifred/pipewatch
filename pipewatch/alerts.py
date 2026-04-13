"""Alert notification dispatch for pipewatch."""
from __future__ import annotations

import smtplib
import logging
from dataclasses import dataclass, field
from email.message import EmailMessage
from typing import List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus

logger = logging.getLogger(__name__)


@dataclass
class AlertEvent:
    """Represents a single alert that was fired."""
    pipeline_name: str
    level: AlertLevel
    message: str
    timestamp: str


@dataclass
class AlertConfig:
    """Configuration for alert notifications."""
    email_to: List[str] = field(default_factory=list)
    email_from: str = "pipewatch@localhost"
    smtp_host: str = "localhost"
    smtp_port: int = 25
    min_level: AlertLevel = AlertLevel.WARNING


def should_fire(status: PipelineStatus, min_level: AlertLevel) -> bool:
    """Return True if the status level meets or exceeds the minimum alert level."""
    order = [AlertLevel.OK, AlertLevel.WARNING, AlertLevel.CRITICAL]
    return order.index(status.level) >= order.index(min_level)


def build_alert_event(status: PipelineStatus, timestamp: str) -> AlertEvent:
    """Build an AlertEvent from a PipelineStatus."""
    return AlertEvent(
        pipeline_name=status.pipeline_name,
        level=status.level,
        message=status.message or "",
        timestamp=timestamp,
    )


def send_email_alert(
    event: AlertEvent,
    config: AlertConfig,
    smtp_cls=smtplib.SMTP,
) -> bool:
    """Send an email notification for an alert event. Returns True on success."""
    if not config.email_to:
        logger.debug("No email recipients configured; skipping email alert.")
        return False

    subject = f"[pipewatch] {event.level.value.upper()} — {event.pipeline_name}"
    body = (
        f"Pipeline : {event.pipeline_name}\n"
        f"Level    : {event.level.value.upper()}\n"
        f"Message  : {event.message}\n"
        f"Time     : {event.timestamp}\n"
    )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = config.email_from
    msg["To"] = ", ".join(config.email_to)
    msg.set_content(body)

    try:
        with smtp_cls(config.smtp_host, config.smtp_port) as smtp:
            smtp.send_message(msg)
        logger.info("Alert email sent for pipeline '%s'.", event.pipeline_name)
        return True
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to send alert email: %s", exc)
        return False


def dispatch_alerts(
    statuses: List[PipelineStatus],
    config: AlertConfig,
    timestamp: str,
    smtp_cls=smtplib.SMTP,
) -> List[AlertEvent]:
    """Evaluate all pipeline statuses and dispatch alerts as needed."""
    fired: List[AlertEvent] = []
    for status in statuses:
        if should_fire(status, config.min_level):
            event = build_alert_event(status, timestamp)
            send_email_alert(event, config, smtp_cls=smtp_cls)
            fired.append(event)
    return fired
