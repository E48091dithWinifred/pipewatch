"""Compare pipeline statuses across two runs and report changes."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class PipelineChange:
    name: str
    previous_level: AlertLevel
    current_level: AlertLevel
    previous_error_rate: float
    current_error_rate: float

    @property
    def degraded(self) -> bool:
        order = [AlertLevel.OK, AlertLevel.WARNING, AlertLevel.CRITICAL]
        return order.index(self.current_level) > order.index(self.previous_level)

    @property
    def improved(self) -> bool:
        order = [AlertLevel.OK, AlertLevel.WARNING, AlertLevel.CRITICAL]
        return order.index(self.current_level) < order.index(self.previous_level)

    @property
    def error_rate_delta(self) -> float:
        return self.current_error_rate - self.previous_error_rate


@dataclass
class ComparisonReport:
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    changes: List[PipelineChange] = field(default_factory=list)

    @property
    def degraded(self) -> List[PipelineChange]:
        return [c for c in self.changes if c.degraded]

    @property
    def improved(self) -> List[PipelineChange]:
        return [c for c in self.changes if c.improved]

    @property
    def has_regressions(self) -> bool:
        return bool(self.degraded or self.added)


def compare_runs(
    previous: List[PipelineStatus],
    current: List[PipelineStatus],
) -> ComparisonReport:
    """Compare two lists of PipelineStatus and return a ComparisonReport."""
    prev_map: Dict[str, PipelineStatus] = {s.pipeline_name: s for s in previous}
    curr_map: Dict[str, PipelineStatus] = {s.pipeline_name: s for s in current}

    added = [name for name in curr_map if name not in prev_map]
    removed = [name for name in prev_map if name not in curr_map]

    changes: List[PipelineChange] = []
    for name in curr_map:
        if name not in prev_map:
            continue
        prev_s = prev_map[name]
        curr_s = curr_map[name]
        if prev_s.level != curr_s.level or prev_s.error_rate != curr_s.error_rate:
            changes.append(
                PipelineChange(
                    name=name,
                    previous_level=prev_s.level,
                    current_level=curr_s.level,
                    previous_error_rate=prev_s.error_rate,
                    current_error_rate=curr_s.error_rate,
                )
            )

    return ComparisonReport(added=added, removed=removed, changes=changes)
