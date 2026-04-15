"""Reaper: removes stale pipeline statuses based on last-seen age."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.checker import PipelineStatus


@dataclass
class ReaperConfig:
    max_age_seconds: float = 3600.0
    dry_run: bool = False


@dataclass
class ReapResult:
    kept: List[PipelineStatus] = field(default_factory=list)
    reaped: List[PipelineStatus] = field(default_factory=list)

    @property
    def reaped_count(self) -> int:
        return len(self.reaped)

    @property
    def kept_count(self) -> int:
        return len(self.kept)

    def summary(self) -> str:
        return f"reaped={self.reaped_count} kept={self.kept_count}"


def _age_seconds(ts: str, now: datetime) -> float:
    """Return age in seconds for an ISO-8601 timestamp string."""
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (now - dt).total_seconds()
    except (ValueError, TypeError):
        return float("inf")


def reap_statuses(
    statuses: List[PipelineStatus],
    config: Optional[ReaperConfig] = None,
    now: Optional[datetime] = None,
) -> ReapResult:
    """Filter out statuses whose checked_at timestamp is older than max_age_seconds."""
    if config is None:
        config = ReaperConfig()
    if now is None:
        now = datetime.now(timezone.utc)

    result = ReapResult()
    for status in statuses:
        ts = getattr(status, "checked_at", None)
        age = _age_seconds(ts, now) if ts else float("inf")
        if age <= config.max_age_seconds:
            result.kept.append(status)
        else:
            result.reaped.append(status)
    return result
