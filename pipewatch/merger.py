"""merger.py — Merge multiple pipeline status lists into a unified deduplicated view."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict, Optional

from pipewatch.checker import PipelineStatus, AlertLevel


@dataclass
class MergeConfig:
    strategy: str = "latest"  # "latest" | "worst" | "first"
    deduplicate: bool = True


@dataclass
class MergeResult:
    statuses: List[PipelineStatus]
    source_counts: Dict[str, int] = field(default_factory=dict)
    duplicate_count: int = 0

    @property
    def total_merged(self) -> int:
        return len(self.statuses)

    def summary(self) -> str:
        return (
            f"Merged {self.total_merged} pipelines "
            f"({self.duplicate_count} duplicates resolved) "
            f"from {len(self.source_counts)} source(s)"
        )


_LEVEL_ORDER: Dict[AlertLevel, int] = {
    AlertLevel.OK: 0,
    AlertLevel.WARNING: 1,
    AlertLevel.CRITICAL: 2,
}


def _pick(existing: PipelineStatus, incoming: PipelineStatus, strategy: str) -> PipelineStatus:
    if strategy == "worst":
        return incoming if _LEVEL_ORDER[incoming.level] > _LEVEL_ORDER[existing.level] else existing
    if strategy == "first":
        return existing
    # default: "latest" — incoming wins
    return incoming


def merge_statuses(
    sources: List[List[PipelineStatus]],
    config: Optional[MergeConfig] = None,
) -> MergeResult:
    """Merge multiple lists of PipelineStatus into one."""
    if config is None:
        config = MergeConfig()

    seen: Dict[str, PipelineStatus] = {}
    source_counts: Dict[str, int] = {}
    duplicate_count = 0

    for idx, source in enumerate(sources):
        label = f"source_{idx}"
        source_counts[label] = len(source)
        for status in source:
            if config.deduplicate and status.pipeline_name in seen:
                duplicate_count += 1
                seen[status.pipeline_name] = _pick(seen[status.pipeline_name], status, config.strategy)
            else:
                seen[status.pipeline_name] = status

    return MergeResult(
        statuses=list(seen.values()),
        source_counts=source_counts,
        duplicate_count=duplicate_count,
    )
