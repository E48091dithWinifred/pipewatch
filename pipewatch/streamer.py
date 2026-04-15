"""Stream pipeline statuses through a processing pipeline with backpressure support."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable, Iterator, List, Optional

from pipewatch.checker import PipelineStatus


@dataclass
class StreamConfig:
    batch_size: int = 10
    max_items: Optional[int] = None
    drop_ok: bool = False


@dataclass
class StreamResult:
    emitted: List[PipelineStatus] = field(default_factory=list)
    dropped: int = 0

    @property
    def total_seen(self) -> int:
        return len(self.emitted) + self.dropped

    def summary(self) -> str:
        return (
            f"emitted={len(self.emitted)} "
            f"dropped={self.dropped} "
            f"total={self.total_seen}"
        )


def _should_drop(status: PipelineStatus, cfg: StreamConfig) -> bool:
    if cfg.drop_ok and status.level.value == "ok":
        return True
    return False


def stream_statuses(
    source: Iterable[PipelineStatus],
    cfg: Optional[StreamConfig] = None,
    on_emit: Optional[Callable[[PipelineStatus], None]] = None,
) -> StreamResult:
    """Stream statuses through config-driven filtering with optional callback."""
    if cfg is None:
        cfg = StreamConfig()

    result = StreamResult()

    for status in source:
        if cfg.max_items is not None and len(result.emitted) >= cfg.max_items:
            result.dropped += 1
            continue

        if _should_drop(status, cfg):
            result.dropped += 1
            continue

        result.emitted.append(status)
        if on_emit is not None:
            on_emit(status)

    return result


def batch_stream(
    source: Iterable[PipelineStatus],
    batch_size: int = 10,
) -> Iterator[List[PipelineStatus]]:
    """Yield successive batches from a stream of statuses."""
    batch: List[PipelineStatus] = []
    for status in source:
        batch.append(status)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
