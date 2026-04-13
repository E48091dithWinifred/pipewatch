"""Rank pipelines by health score and surface the worst offenders."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.checker import PipelineStatus, AlertLevel
from pipewatch.scorer import PipelineScore, score_pipeline


@dataclass
class RankedPipeline:
    """A pipeline with its computed rank position and score."""

    rank: int
    name: str
    level: AlertLevel
    score: float
    grade: str
    error_rate: float
    latency_ms: float

    @property
    def is_critical(self) -> bool:
        return self.level == AlertLevel.CRITICAL

    @property
    def is_healthy(self) -> bool:
        return self.level == AlertLevel.OK


def _pipeline_sort_key(ps: PipelineScore) -> tuple:
    """Lower score = worse = ranked first (ascending by score)."""
    level_order = {
        AlertLevel.CRITICAL: 0,
        AlertLevel.WARNING: 1,
        AlertLevel.OK: 2,
    }
    return (level_order.get(ps.status.level, 2), ps.score)


def rank_pipelines(
    statuses: List[PipelineStatus],
    top_n: Optional[int] = None,
) -> List[RankedPipeline]:
    """Rank pipelines from worst to best health.

    Args:
        statuses: List of pipeline statuses to rank.
        top_n: If set, return only the top_n worst pipelines.

    Returns:
        Ordered list of RankedPipeline, rank 1 = worst.
    """
    if not statuses:
        return []

    scored = [score_pipeline(s) for s in statuses]
    scored.sort(key=_pipeline_sort_key)

    if top_n is not None:
        scored = scored[:top_n]

    return [
        RankedPipeline(
            rank=idx + 1,
            name=ps.status.pipeline_name,
            level=ps.status.level,
            score=ps.score,
            grade=ps.grade,
            error_rate=ps.status.metrics.error_rate if ps.status.metrics else 0.0,
            latency_ms=ps.status.metrics.latency_ms if ps.status.metrics else 0.0,
        )
        for idx, ps in enumerate(scored)
    ]


def worst_pipeline(statuses: List[PipelineStatus]) -> Optional[RankedPipeline]:
    """Return the single worst-ranked pipeline, or None if list is empty."""
    ranked = rank_pipelines(statuses, top_n=1)
    return ranked[0] if ranked else None
