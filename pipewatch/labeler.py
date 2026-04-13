"""pipewatch/labeler.py — Assigns human-readable labels and tags to pipeline statuses."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class LabeledStatus:
    pipeline_name: str
    level: AlertLevel
    tags: List[str] = field(default_factory=list)
    badge: str = ""
    summary: str = ""


_BADGES: dict[AlertLevel, str] = {
    AlertLevel.OK: "✅",
    AlertLevel.WARNING: "⚠️",
    AlertLevel.CRITICAL: "🔴",
}

_TAG_RULES: list[tuple[str, object]] = [
    ("high-error-rate", lambda s: s.error_rate is not None and s.error_rate >= 0.05),
    ("slow", lambda s: s.latency_ms is not None and s.latency_ms >= 1000),
    ("critical", lambda s: s.level == AlertLevel.CRITICAL),
    ("warning", lambda s: s.level == AlertLevel.WARNING),
    ("healthy", lambda s: s.level == AlertLevel.OK),
]


def _build_tags(status: PipelineStatus) -> List[str]:
    return [tag for tag, rule in _TAG_RULES if rule(status)]


def _build_summary(status: PipelineStatus) -> str:
    parts: list[str] = []
    if status.error_rate is not None:
        parts.append(f"error_rate={status.error_rate:.2%}")
    if status.latency_ms is not None:
        parts.append(f"latency={status.latency_ms:.0f}ms")
    if status.message:
        parts.append(status.message)
    return "; ".join(parts) if parts else "no issues"


def label_status(status: PipelineStatus) -> LabeledStatus:
    """Attach badge, tags, and summary to a PipelineStatus."""
    return LabeledStatus(
        pipeline_name=status.pipeline_name,
        level=status.level,
        tags=_build_tags(status),
        badge=_BADGES.get(status.level, "?"),
        summary=_build_summary(status),
    )


def label_all(statuses: List[PipelineStatus]) -> List[LabeledStatus]:
    """Label a list of pipeline statuses."""
    return [label_status(s) for s in statuses]


def filter_by_tag(labeled: List[LabeledStatus], tag: str) -> List[LabeledStatus]:
    """Return only labeled statuses that carry the given tag."""
    return [ls for ls in labeled if tag in ls.tags]
