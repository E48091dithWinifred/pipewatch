"""inverter.py — Invert a filter: keep only statuses that would have been dropped."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.checker import PipelineStatus
from pipewatch.filter import FilterConfig, apply_filter


@dataclass
class InvertResult:
    kept: List[PipelineStatus] = field(default_factory=list)
    dropped: List[PipelineStatus] = field(default_factory=list)

    @property
    def kept_count(self) -> int:
        return len(self.kept)

    @property
    def dropped_count(self) -> int:
        return len(self.dropped)

    def summary(self) -> str:
        return f"InvertResult: kept={self.kept_count}, dropped={self.dropped_count}"


def invert_filter(
    statuses: List[PipelineStatus],
    config: FilterConfig,
) -> InvertResult:
    """Return statuses that do NOT pass *config*.

    The statuses that would normally be kept by apply_filter become the
    'dropped' set; those that would be dropped become 'kept'.
    """
    passed = set(id(s) for s in apply_filter(statuses, config))
    kept = [s for s in statuses if id(s) not in passed]
    dropped = [s for s in statuses if id(s) in passed]
    return InvertResult(kept=kept, dropped=dropped)
