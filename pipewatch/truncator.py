"""truncator.py — Truncate a list of pipeline statuses to the first N items."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.checker import PipelineStatus


@dataclass
class TruncateConfig:
    max_items: int = 10

    def __post_init__(self) -> None:
        if self.max_items < 1:
            raise ValueError("max_items must be at least 1")


@dataclass
class TruncateResult:
    items: List[PipelineStatus]
    total_input: int
    max_items: int

    @property
    def kept_count(self) -> int:
        return len(self.items)

    @property
    def dropped_count(self) -> int:
        return max(0, self.total_input - self.kept_count)

    @property
    def was_truncated(self) -> bool:
        return self.dropped_count > 0

    def summary(self) -> str:
        if self.was_truncated:
            return (
                f"Truncated to {self.kept_count} of {self.total_input} pipelines "
                f"(dropped {self.dropped_count})"
            )
        return f"Kept all {self.kept_count} pipelines (no truncation needed)"


def truncate_statuses(
    statuses: List[PipelineStatus],
    config: TruncateConfig | None = None,
) -> TruncateResult:
    """Return at most config.max_items statuses from the front of *statuses*."""
    if config is None:
        config = TruncateConfig()
    total = len(statuses)
    kept = statuses[: config.max_items]
    return TruncateResult(items=kept, total_input=total, max_items=config.max_items)
