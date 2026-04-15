"""splitter.py — Split a list of pipeline statuses into partitions based on a field or predicate."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.checker import PipelineStatus, AlertLevel


@dataclass
class SplitResult:
    partitions: Dict[str, List[PipelineStatus]] = field(default_factory=dict)

    @property
    def partition_names(self) -> List[str]:
        return list(self.partitions.keys())

    @property
    def total_count(self) -> int:
        return sum(len(v) for v in self.partitions.values())

    def get(self, key: str) -> List[PipelineStatus]:
        return self.partitions.get(key, [])

    def summary(self) -> str:
        parts = ", ".join(
            f"{k}={len(v)}" for k, v in sorted(self.partitions.items())
        )
        return f"SplitResult({parts})"


def split_by_level(statuses: List[PipelineStatus]) -> SplitResult:
    """Partition statuses by their AlertLevel value."""
    partitions: Dict[str, List[PipelineStatus]] = {}
    for status in statuses:
        key = status.level.value
        partitions.setdefault(key, []).append(status)
    return SplitResult(partitions=partitions)


def split_by_prefix(
    statuses: List[PipelineStatus],
    delimiter: str = "_",
) -> SplitResult:
    """Partition statuses by the first segment of their name split on *delimiter*."""
    partitions: Dict[str, List[PipelineStatus]] = {}
    for status in statuses:
        prefix = status.pipeline_name.split(delimiter)[0]
        partitions.setdefault(prefix, []).append(status)
    return SplitResult(partitions=partitions)


def split_by_predicate(
    statuses: List[PipelineStatus],
    predicate: Callable[[PipelineStatus], str],
) -> SplitResult:
    """Partition statuses using an arbitrary callable that returns a bucket key."""
    partitions: Dict[str, List[PipelineStatus]] = {}
    for status in statuses:
        key = predicate(status)
        partitions.setdefault(key, []).append(status)
    return SplitResult(partitions=partitions)
