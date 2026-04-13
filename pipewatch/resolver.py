"""Resolve pipeline statuses to recommended actions based on level and context."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class ResolvedAction:
    pipeline_name: str
    level: str
    action: str
    priority: int  # 1 = highest
    notes: List[str] = field(default_factory=list)

    @property
    def is_urgent(self) -> bool:
        return self.priority == 1

    def summary(self) -> str:
        base = f"[P{self.priority}] {self.pipeline_name}: {self.action}"
        if self.notes:
            return base + " (" + "; ".join(self.notes) + ")"
        return base


_ACTION_MAP = {
    AlertLevel.OK: ("monitor", 3),
    AlertLevel.WARNING: ("investigate", 2),
    AlertLevel.CRITICAL: ("remediate immediately", 1),
}


def resolve_action(status: PipelineStatus) -> ResolvedAction:
    """Derive a recommended action and priority from a pipeline status."""
    action, priority = _ACTION_MAP.get(status.level, ("review", 3))
    notes: List[str] = []

    if status.error_rate is not None and status.error_rate > 0.1:
        notes.append(f"error rate {status.error_rate:.1%}")
    if status.latency_ms is not None and status.latency_ms > 5000:
        notes.append(f"latency {status.latency_ms:.0f}ms")
    if status.message:
        notes.append(status.message)

    return ResolvedAction(
        pipeline_name=status.pipeline_name,
        level=status.level.value,
        action=action,
        priority=priority,
        notes=notes,
    )


def resolve_all(
    statuses: List[PipelineStatus],
    min_priority: Optional[int] = None,
) -> List[ResolvedAction]:
    """Resolve actions for all statuses, optionally filtered by min priority."""
    actions = [resolve_action(s) for s in statuses]
    if min_priority is not None:
        actions = [a for a in actions if a.priority <= min_priority]
    return sorted(actions, key=lambda a: (a.priority, a.pipeline_name))
