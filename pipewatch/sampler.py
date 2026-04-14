"""pipewatch/sampler.py — Random and deterministic sampling of pipeline statuses."""
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import PipelineStatus


@dataclass
class SamplerConfig:
    rate: float = 1.0          # fraction 0.0–1.0 to keep
    seed: Optional[int] = None # if set, use deterministic sampling
    min_keep: int = 0          # always keep at least this many results

    def __post_init__(self) -> None:
        if not (0.0 <= self.rate <= 1.0):
            raise ValueError(f"rate must be between 0.0 and 1.0, got {self.rate}")
        if self.min_keep < 0:
            raise ValueError("min_keep must be >= 0")


@dataclass
class SampleResult:
    kept: List[PipelineStatus]
    dropped: List[PipelineStatus]

    @property
    def kept_count(self) -> int:
        return len(self.kept)

    @property
    def dropped_count(self) -> int:
        return len(self.dropped)

    @property
    def summary(self) -> str:
        total = self.kept_count + self.dropped_count
        return f"kept {self.kept_count}/{total} pipelines after sampling"


def _deterministic_keep(status: PipelineStatus, rate: float, seed: int) -> bool:
    """Hash pipeline name + seed to decide deterministically."""
    digest = hashlib.md5(f"{seed}:{status.pipeline_name}".encode()).hexdigest()
    value = int(digest[:8], 16) / 0xFFFFFFFF
    return value < rate


def sample_statuses(
    statuses: List[PipelineStatus],
    config: SamplerConfig,
) -> SampleResult:
    """Return a SampleResult partitioning statuses into kept/dropped."""
    if not statuses:
        return SampleResult(kept=[], dropped=[])

    if config.rate >= 1.0:
        return SampleResult(kept=list(statuses), dropped=[])

    rng = random.Random(config.seed)
    kept: List[PipelineStatus] = []
    dropped: List[PipelineStatus] = []

    for status in statuses:
        if config.seed is not None:
            keep = _deterministic_keep(status, config.rate, config.seed)
        else:
            keep = rng.random() < config.rate

        if keep:
            kept.append(status)
        else:
            dropped.append(status)

    # honour min_keep by pulling back from dropped (preserve order)
    while len(kept) < config.min_keep and dropped:
        kept.append(dropped.pop(0))

    return SampleResult(kept=kept, dropped=dropped)
