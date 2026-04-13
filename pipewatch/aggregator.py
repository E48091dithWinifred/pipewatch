"""Aggregates pipeline statuses into summary statistics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class AggregatedStats:
    total: int = 0
    ok_count: int = 0
    warning_count: int = 0
    critical_count: int = 0
    avg_error_rate: float = 0.0
    avg_latency_ms: float = 0.0
    max_error_rate: float = 0.0
    max_latency_ms: float = 0.0
    pipeline_names: List[str] = field(default_factory=list)

    @property
    def health_ratio(self) -> float:
        """Fraction of pipelines that are OK."""
        if self.total == 0:
            return 1.0
        return self.ok_count / self.total

    @property
    def level_counts(self) -> Dict[str, int]:
        return {
            AlertLevel.OK.value: self.ok_count,
            AlertLevel.WARNING.value: self.warning_count,
            AlertLevel.CRITICAL.value: self.critical_count,
        }


def aggregate(statuses: List[PipelineStatus]) -> AggregatedStats:
    """Compute aggregate statistics from a list of pipeline statuses."""
    if not statuses:
        return AggregatedStats()

    ok = sum(1 for s in statuses if s.level == AlertLevel.OK)
    warning = sum(1 for s in statuses if s.level == AlertLevel.WARNING)
    critical = sum(1 for s in statuses if s.level == AlertLevel.CRITICAL)

    error_rates = [s.metrics.error_rate for s in statuses]
    latencies = [s.metrics.latency_ms for s in statuses]

    return AggregatedStats(
        total=len(statuses),
        ok_count=ok,
        warning_count=warning,
        critical_count=critical,
        avg_error_rate=sum(error_rates) / len(error_rates),
        avg_latency_ms=sum(latencies) / len(latencies),
        max_error_rate=max(error_rates),
        max_latency_ms=max(latencies),
        pipeline_names=[s.pipeline_name for s in statuses],
    )


def group_by_level(
    statuses: List[PipelineStatus],
) -> Dict[str, List[PipelineStatus]]:
    """Group pipeline statuses by their alert level."""
    groups: Dict[str, List[PipelineStatus]] = {
        AlertLevel.OK.value: [],
        AlertLevel.WARNING.value: [],
        AlertLevel.CRITICAL.value: [],
    }
    for status in statuses:
        groups[status.level.value].append(status)
    return groups
