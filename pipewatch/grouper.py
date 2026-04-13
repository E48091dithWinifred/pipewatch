"""Group pipeline statuses by a given attribute for batch analysis."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class PipelineGroup:
    """A named collection of pipeline statuses sharing a common key."""

    key: str
    statuses: List[PipelineStatus] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.statuses)

    @property
    def critical_count(self) -> int:
        return sum(1 for s in self.statuses if s.level == AlertLevel.CRITICAL)

    @property
    def warning_count(self) -> int:
        return sum(1 for s in self.statuses if s.level == AlertLevel.WARNING)

    @property
    def ok_count(self) -> int:
        return sum(1 for s in self.statuses if s.level == AlertLevel.OK)

    @property
    def avg_error_rate(self) -> float:
        if not self.statuses:
            return 0.0
        return sum(s.error_rate for s in self.statuses) / len(self.statuses)

    @property
    def worst_level(self) -> AlertLevel:
        if any(s.level == AlertLevel.CRITICAL for s in self.statuses):
            return AlertLevel.CRITICAL
        if any(s.level == AlertLevel.WARNING for s in self.statuses):
            return AlertLevel.WARNING
        return AlertLevel.OK


def group_by(
    statuses: List[PipelineStatus],
    key_fn: Callable[[PipelineStatus], Optional[str]],
) -> Dict[str, PipelineGroup]:
    """Group statuses by the value returned by *key_fn*.

    Statuses for which *key_fn* returns ``None`` are silently skipped.
    """
    groups: Dict[str, PipelineGroup] = {}
    for status in statuses:
        key = key_fn(status)
        if key is None:
            continue
        if key not in groups:
            groups[key] = PipelineGroup(key=key)
        groups[key].statuses.append(status)
    return groups


def group_by_level(statuses: List[PipelineStatus]) -> Dict[str, PipelineGroup]:
    """Convenience wrapper — groups by alert level name."""
    return group_by(statuses, lambda s: s.level.value)


def group_by_prefix(
    statuses: List[PipelineStatus], separator: str = "_"
) -> Dict[str, PipelineGroup]:
    """Group by the first segment of the pipeline name split on *separator*."""
    def _prefix(s: PipelineStatus) -> Optional[str]:
        parts = s.pipeline_name.split(separator, 1)
        return parts[0] if parts else None

    return group_by(statuses, _prefix)
