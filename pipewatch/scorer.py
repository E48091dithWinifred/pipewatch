"""Pipeline health scorer: assigns a numeric score (0–100) to each pipeline
based on its current status, error rate, and latency thresholds."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus


# Weight constants (must sum to 1.0)
_WEIGHT_LEVEL = 0.50
_WEIGHT_ERROR_RATE = 0.30
_WEIGHT_LATENCY = 0.20


@dataclass(frozen=True)
class PipelineScore:
    pipeline: str
    score: float          # 0.0 – 100.0
    level: AlertLevel
    error_rate: float
    latency_ms: float

    @property
    def grade(self) -> str:
        """Letter grade derived from the numeric score."""
        if self.score >= 90:
            return "A"
        if self.score >= 75:
            return "B"
        if self.score >= 60:
            return "C"
        if self.score >= 40:
            return "D"
        return "F"


def _level_component(level: AlertLevel) -> float:
    """Return a 0–100 sub-score for the alert level."""
    return {AlertLevel.OK: 100.0, AlertLevel.WARNING: 50.0, AlertLevel.CRITICAL: 0.0}[level]


def _error_rate_component(error_rate: float) -> float:
    """Return a 0–100 sub-score; perfect at 0 %, zero at 100 %."""
    return max(0.0, 100.0 - error_rate * 100.0)


def _latency_component(latency_ms: float, ceiling_ms: float = 5000.0) -> float:
    """Return a 0–100 sub-score; perfect at 0 ms, zero at *ceiling_ms*."""
    return max(0.0, 100.0 * (1.0 - latency_ms / ceiling_ms))


def score_pipeline(status: PipelineStatus, latency_ceiling_ms: float = 5000.0) -> PipelineScore:
    """Compute a composite health score for a single pipeline."""
    lc = _level_component(status.level)
    ec = _error_rate_component(status.error_rate)
    lat = _latency_component(status.latency_ms, latency_ceiling_ms)
    composite = lc * _WEIGHT_LEVEL + ec * _WEIGHT_ERROR_RATE + lat * _WEIGHT_LATENCY
    return PipelineScore(
        pipeline=status.pipeline,
        score=round(composite, 2),
        level=status.level,
        error_rate=status.error_rate,
        latency_ms=status.latency_ms,
    )


def score_all(
    statuses: List[PipelineStatus], latency_ceiling_ms: float = 5000.0
) -> List[PipelineScore]:
    """Score every pipeline in *statuses*, sorted best-to-worst."""
    scores = [score_pipeline(s, latency_ceiling_ms) for s in statuses]
    return sorted(scores, key=lambda ps: ps.score, reverse=True)
