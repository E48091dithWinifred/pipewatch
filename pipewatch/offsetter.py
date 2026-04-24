"""offsetter.py — Apply numeric offsets to pipeline metric values."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import PipelineStatus


@dataclass
class OffsetConfig:
    """Configuration for metric offsetting."""
    error_rate_offset: float = 0.0   # additive offset applied to error_rate
    latency_offset: float = 0.0      # additive offset applied to latency_ms
    clamp_min: float = 0.0           # floor after offset is applied

    def __post_init__(self) -> None:
        if self.clamp_min < 0:
            raise ValueError("clamp_min must be >= 0")


@dataclass
class OffsetStatus:
    """A pipeline status with offset metrics applied."""
    original: PipelineStatus
    error_rate: float
    latency_ms: float
    config: OffsetConfig

    @property
    def name(self) -> str:
        return self.original.name

    @property
    def level(self) -> str:
        return self.original.level.value

    @property
    def message(self) -> Optional[str]:
        return self.original.message

    def summary(self) -> str:
        return (
            f"{self.name}: error_rate={self.error_rate:.4f} "
            f"latency_ms={self.latency_ms:.1f} [{self.level}]"
        )

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "level": self.level,
            "error_rate": self.error_rate,
            "latency_ms": self.latency_ms,
            "message": self.message,
        }


@dataclass
class OffsetResult:
    statuses: List[OffsetStatus] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.statuses)

    def summary(self) -> str:
        return f"Offset applied to {self.count} pipeline(s)."


def _clamp(value: float, minimum: float) -> float:
    return max(minimum, value)


def offset_status(status: PipelineStatus, cfg: OffsetConfig) -> OffsetStatus:
    """Apply offset to a single PipelineStatus."""
    raw_error = (status.error_rate or 0.0) + cfg.error_rate_offset
    raw_latency = (status.latency_ms or 0.0) + cfg.latency_offset
    return OffsetStatus(
        original=status,
        error_rate=_clamp(raw_error, cfg.clamp_min),
        latency_ms=_clamp(raw_latency, cfg.clamp_min),
        config=cfg,
    )


def offset_all(
    statuses: List[PipelineStatus], cfg: Optional[OffsetConfig] = None
) -> OffsetResult:
    """Apply offset to a list of pipeline statuses."""
    cfg = cfg or OffsetConfig()
    return OffsetResult(statuses=[offset_status(s, cfg) for s in statuses])
