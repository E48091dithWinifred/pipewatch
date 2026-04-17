"""Pinpointer: identify the single most problematic pipeline from a list of statuses."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List
from pipewatch.checker import AlertLevel, PipelineStatus

_LEVEL_ORDER = {AlertLevel.CRITICAL: 2, AlertLevel.WARNING: 1, AlertLevel.OK: 0}


@dataclass
class PinpointResult:
    pipeline: Optional[PipelineStatus]
    reason: str
    score: float

    def summary(self) -> str:
        if self.pipeline is None:
            return "No problematic pipeline found."
        return (
            f"Pinpointed: {self.pipeline.name} "
            f"[{self.pipeline.level.value}] score={self.score:.3f} — {self.reason}"
        )


def _score(status: PipelineStatus) -> float:
    level_score = _LEVEL_ORDER.get(status.level, 0) * 10.0
    error_score = (status.error_rate or 0.0) * 5.0
    latency_score = (status.latency_ms or 0.0) / 1000.0
    return level_score + error_score + latency_score


def pinpoint(statuses: List[PipelineStatus]) -> PinpointResult:
    """Return the single most problematic pipeline."""
    if not statuses:
        return PinpointResult(pipeline=None, reason="empty input", score=0.0)

    worst = max(statuses, key=_score)
    score = _score(worst)

    if worst.level == AlertLevel.OK and score < 1.0:
        return PinpointResult(pipeline=None, reason="all pipelines healthy", score=score)

    parts = []
    if worst.level == AlertLevel.CRITICAL:
        parts.append("critical alert level")
    elif worst.level == AlertLevel.WARNING:
        parts.append("warning alert level")
    if (worst.error_rate or 0.0) > 0:
        parts.append(f"error_rate={worst.error_rate:.2%}")
    if (worst.latency_ms or 0.0) > 0:
        parts.append(f"latency={worst.latency_ms:.1f}ms")

    reason = ", ".join(parts) if parts else "highest composite score"
    return PinpointResult(pipeline=worst, reason=reason, score=score)
