"""Pipeline status classifier — assigns human-readable categories and severity hints."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class ClassifiedPipeline:
    name: str
    level: AlertLevel
    category: str          # e.g. "throughput", "error_rate", "latency", "healthy"
    severity: int          # 0=ok, 1=warning, 2=critical
    hint: Optional[str]    # short human-readable suggestion

    @property
    def is_actionable(self) -> bool:
        return self.severity > 0


def _categorize(status: PipelineStatus) -> str:
    """Determine the primary failing dimension, or 'healthy'."""
    if status.level == AlertLevel.OK:
        return "healthy"

    msg = (status.message or "").lower()
    if "error" in msg:
        return "error_rate"
    if "latency" in msg:
        return "latency"
    if "throughput" in msg or "rows" in msg:
        return "throughput"
    return "general"


def _severity(level: AlertLevel) -> int:
    return {AlertLevel.OK: 0, AlertLevel.WARNING: 1, AlertLevel.CRITICAL: 2}.get(level, 0)


_HINTS = {
    "error_rate": "Investigate upstream data quality or source system errors.",
    "latency": "Check for slow transforms, network issues, or resource contention.",
    "throughput": "Verify source volume and ingestion parallelism.",
    "general": "Review pipeline logs for recent failures.",
    "healthy": None,
}


def classify_status(status: PipelineStatus) -> ClassifiedPipeline:
    """Classify a single PipelineStatus into a ClassifiedPipeline."""
    category = _categorize(status)
    return ClassifiedPipeline(
        name=status.pipeline_name,
        level=status.level,
        category=category,
        severity=_severity(status.level),
        hint=_HINTS.get(category),
    )


def classify_all(statuses: List[PipelineStatus]) -> List[ClassifiedPipeline]:
    """Classify a list of pipeline statuses."""
    return [classify_status(s) for s in statuses]
