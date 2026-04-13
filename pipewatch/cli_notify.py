"""CLI helpers for the notify sub-command (test / preview notifications)."""
from __future__ import annotations

import argparse
from typing import List

from pipewatch.alerts import AlertEvent
from pipewatch.checker import AlertLevel
from pipewatch.notifier import NotifierConfig, NotificationResult, dispatch


def _build_notify_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("notify", help="Send a test notification via configured channels")
    p.add_argument("pipeline", help="Pipeline name to include in the test alert")
    p.add_argument(
        "--level",
        choices=["ok", "warning", "critical"],
        default="warning",
        help="Alert level for the test event (default: warning)",
    )
    p.add_argument("--no-log", dest="log_enabled", action="store_false", default=True)
    p.add_argument("--email", dest="email_enabled", action="store_true", default=False)
    p.add_argument("--webhook-url", dest="webhook_url", default=None)


_LEVEL_MAP = {
    "ok": AlertLevel.OK,
    "warning": AlertLevel.WARNING,
    "critical": AlertLevel.CRITICAL,
}


def _level_from_str(value: str) -> AlertLevel:
    """Convert a string level name to an :class:`AlertLevel` enum value.

    Raises ``KeyError`` if *value* is not one of ``ok``, ``warning``, or
    ``critical``.
    """
    try:
        return _LEVEL_MAP[value]
    except KeyError:
        valid = ", ".join(_LEVEL_MAP)
        raise ValueError(f"Unknown alert level {value!r}. Valid choices: {valid}") from None


def cmd_notify(args: argparse.Namespace) -> List[NotificationResult]:
    """Execute the notify sub-command; returns results for testing."""
    from datetime import datetime, timezone

    event = AlertEvent(
        pipeline_name=args.pipeline,
        level=_level_from_str(args.level),
        message=f"Test notification triggered via CLI for pipeline '{args.pipeline}'.",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )

    cfg = NotifierConfig(
        log_enabled=getattr(args, "log_enabled", True),
        email_enabled=getattr(args, "email_enabled", False),
        webhook_enabled=bool(getattr(args, "webhook_url", None)),
        webhook_url=getattr(args, "webhook_url", None),
    )

    results = dispatch(event, cfg)

    failed = [r for r in results if not r.success]
    if failed:
        for r in failed:
            print(f"  [FAILED] {r.channel}: {r.message}")
    else:
        print(f"  Dispatched to {len(results)} channel(s) successfully.")

    return results
