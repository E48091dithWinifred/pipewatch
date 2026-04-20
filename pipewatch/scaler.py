"""scaler.py — Normalize pipeline metric values to a 0–1 scale for comparison."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.checker import PipelineStatus


@dataclass
class ScalerConfig:
    max_error_rate: float = 1.0
    max_latency_ms: float = 10_000.0

    def __post_init__(self) -> None:
        if self.max_error_rate <= 0:
            raise ValueError("max_error_rate must be positive")
        if self.max_latency_ms <= 0:
            raise ValueError("max_latency_ms must be positive")


@dataclass
class ScaledStatus:
    name: str
    level: str
    error_rate_scaled: float   # 0.0 – 1.0
    latency_scaled: float      # 0.0 – 1.0
    composite_score: float     # average of the two scaled values

    def summary(self) -> str:
        return (
            f"{self.name} [{self.level}] "
            f"err={self.error_rate_scaled:.3f} "
            f"lat={self.latency_scaled:.3f} "
            f"score={self.composite_score:.3f}"
        )


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def scale_status(status: PipelineStatus, config: Optional[ScalerConfig] = None) -> ScaledStatus:
    """Return a ScaledStatus with metric values normalised to [0, 1]."""
    cfg = config or ScalerConfig()
    err_scaled = _clamp(status.error_rate / cfg.max_error_rate)
    lat_scaled = _clamp(status.latency_ms / cfg.max_latency_ms)
    composite = (err_scaled + lat_scaled) / 2.0
    return ScaledStatus(
        name=status.pipeline_name,
        level=status.level.value,
        error_rate_scaled=err_scaled,
        latency_scaled=lat_scaled,
        composite_score=composite,
    )


def scale_all(statuses: List[PipelineStatus], config: Optional[ScalerConfig] = None) -> List[ScaledStatus]:
    """Scale a list of pipeline statuses."""
    cfg = config or ScalerConfig()
    return [scale_status(s, cfg) for s in statuses]
