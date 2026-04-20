"""pipewatch.clamper — Clamp pipeline metric values to configured min/max bounds."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import PipelineStatus


@dataclass
class ClampConfig:
    min_error_rate: float = 0.0
    max_error_rate: float = 1.0
    min_latency_ms: float = 0.0
    max_latency_ms: float = 60_000.0

    def __post_init__(self) -> None:
        if self.min_error_rate > self.max_error_rate:
            raise ValueError("min_error_rate must be <= max_error_rate")
        if self.min_latency_ms > self.max_latency_ms:
            raise ValueError("min_latency_ms must be <= max_latency_ms")


@dataclass
class ClampedStatus:
    """A pipeline status whose numeric metrics have been clamped."""
    original: PipelineStatus
    error_rate: float
    latency_ms: float
    clamped_fields: List[str] = field(default_factory=list)

    @property
    def name(self) -> str:
        return self.original.name

    @property
    def level(self):
        return self.original.level

    def was_clamped(self) -> bool:
        return len(self.clamped_fields) > 0

    def summary(self) -> str:
        if self.clamped_fields:
            fields = ", ".join(self.clamped_fields)
            return f"{self.name}: clamped [{fields}]"
        return f"{self.name}: no clamping applied"


@dataclass
class ClampResult:
    statuses: List[ClampedStatus] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.statuses)

    @property
    def clamped_count(self) -> int:
        return sum(1 for s in self.statuses if s.was_clamped())

    def summary(self) -> str:
        return f"clamped {self.clamped_count}/{self.total} statuses"


def clamp_status(status: PipelineStatus, cfg: ClampConfig) -> ClampedStatus:
    """Clamp a single status's metrics to the bounds defined in cfg."""
    clamped: List[str] = []

    raw_er = status.error_rate or 0.0
    new_er = max(cfg.min_error_rate, min(cfg.max_error_rate, raw_er))
    if new_er != raw_er:
        clamped.append("error_rate")

    raw_lat = status.latency_ms or 0.0
    new_lat = max(cfg.min_latency_ms, min(cfg.max_latency_ms, raw_lat))
    if new_lat != raw_lat:
        clamped.append("latency_ms")

    return ClampedStatus(
        original=status,
        error_rate=new_er,
        latency_ms=new_lat,
        clamped_fields=clamped,
    )


def clamp_statuses(
    statuses: List[PipelineStatus],
    cfg: Optional[ClampConfig] = None,
) -> ClampResult:
    """Clamp all statuses and return a ClampResult."""
    if cfg is None:
        cfg = ClampConfig()
    return ClampResult(statuses=[clamp_status(s, cfg) for s in statuses])
