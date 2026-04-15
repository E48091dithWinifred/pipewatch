"""pipewatch.capper — Cap (limit) the number of pipeline statuses emitted.

Useful for bounding output size in large deployments.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import PipelineStatus, AlertLevel


@dataclass
class CapConfig:
    max_results: int = 50
    prefer_critical: bool = True  # surface critical entries first when trimming

    def __post_init__(self) -> None:
        if self.max_results < 1:
            raise ValueError("max_results must be at least 1")


@dataclass
class CapResult:
    kept: List[PipelineStatus]
    dropped: List[PipelineStatus]
    cap_applied: bool

    @property
    def kept_count(self) -> int:
        return len(self.kept)

    @property
    def dropped_count(self) -> int:
        return len(self.dropped)

    def summary(self) -> str:
        if not self.cap_applied:
            return f"cap not applied — {self.kept_count} statuses kept"
        return (
            f"cap applied — kept {self.kept_count}, "
            f"dropped {self.dropped_count}"
        )


_LEVEL_PRIORITY = {
    AlertLevel.CRITICAL: 0,
    AlertLevel.WARNING: 1,
    AlertLevel.OK: 2,
}


def cap_statuses(
    statuses: List[PipelineStatus],
    config: Optional[CapConfig] = None,
) -> CapResult:
    """Return at most *config.max_results* statuses.

    When *prefer_critical* is True the list is sorted so that critical
    pipelines are retained before warning, then OK.
    """
    if config is None:
        config = CapConfig()

    if len(statuses) <= config.max_results:
        return CapResult(kept=list(statuses), dropped=[], cap_applied=False)

    ordered = statuses
    if config.prefer_critical:
        ordered = sorted(
            statuses,
            key=lambda s: _LEVEL_PRIORITY.get(s.level, 99),
        )

    kept = ordered[: config.max_results]
    dropped = ordered[config.max_results :]
    return CapResult(kept=kept, dropped=dropped, cap_applied=True)
