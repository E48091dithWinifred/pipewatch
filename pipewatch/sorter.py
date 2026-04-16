"""Sort pipeline statuses by configurable criteria."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.checker import PipelineStatus, AlertLevel

_LEVEL_ORDER = {AlertLevel.CRITICAL: 0, AlertLevel.WARNING: 1, AlertLevel.OK: 2}


@dataclass
class SortConfig:
    key: str = "level"          # level | error_rate | latency | name
    reverse: bool = False

    def __post_init__(self) -> None:
        valid = {"level", "error_rate", "latency", "name"}
        if self.key not in valid:
            raise ValueError(f"sort key must be one of {valid}, got {self.key!r}")


@dataclass
class SortResult:
    items: List[PipelineStatus]
    key: str
    reverse: bool

    @property
    def count(self) -> int:
        return len(self.items)

    def summary(self) -> str:
        direction = "desc" if self.reverse else "asc"
        return f"Sorted {self.count} pipeline(s) by '{self.key}' ({direction})"


def _sort_key(status: PipelineStatus, key: str):
    if key == "level":
        return _LEVEL_ORDER.get(status.level, 99)
    if key == "error_rate":
        return status.error_rate or 0.0
    if key == "latency":
        return status.latency_ms or 0.0
    return status.pipeline_name.lower()


def sort_statuses(
    statuses: List[PipelineStatus],
    config: Optional[SortConfig] = None,
) -> SortResult:
    cfg = config or SortConfig()
    sorted_items = sorted(
        statuses,
        key=lambda s: _sort_key(s, cfg.key),
        reverse=cfg.reverse,
    )
    return SortResult(items=sorted_items, key=cfg.key, reverse=cfg.reverse)
