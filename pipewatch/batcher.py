"""Batch pipeline statuses into fixed-size chunks for bulk processing."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Iterator
from pipewatch.checker import PipelineStatus


@dataclass
class BatchConfig:
    size: int = 10

    def __post_init__(self) -> None:
        if self.size < 1:
            raise ValueError("Batch size must be at least 1")


@dataclass
class Batch:
    index: int
    items: List[PipelineStatus] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.items)

    @property
    def summary(self) -> str:
        return f"Batch {self.index}: {self.count} pipeline(s)"


@dataclass
class BatchResult:
    batches: List[Batch] = field(default_factory=list)

    @property
    def total_batches(self) -> int:
        return len(self.batches)

    @property
    def total_items(self) -> int:
        return sum(b.count for b in self.batches)

    @property
    def summary(self) -> str:
        return f"{self.total_items} item(s) across {self.total_batches} batch(es)"


def _iter_batches(
    statuses: List[PipelineStatus], size: int
) -> Iterator[List[PipelineStatus]]:
    for i in range(0, len(statuses), size):
        yield statuses[i : i + size]


def batch_statuses(
    statuses: List[PipelineStatus],
    config: BatchConfig | None = None,
) -> BatchResult:
    cfg = config or BatchConfig()
    result = BatchResult()
    for idx, chunk in enumerate(_iter_batches(statuses, cfg.size), start=1):
        result.batches.append(Batch(index=idx, items=chunk))
    return result
