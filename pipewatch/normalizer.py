"""Normalize pipeline statuses to a consistent schema for downstream processing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class NormalizedStatus:
    """A pipeline status with guaranteed non-None fields and normalized values."""

    name: str
    level: str
    error_rate: float
    latency_ms: float
    message: str
    tags: List[str] = field(default_factory=list)

    @property
    def is_healthy(self) -> bool:
        return self.level == AlertLevel.OK.value

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "level": self.level,
            "error_rate": self.error_rate,
            "latency_ms": self.latency_ms,
            "message": self.message,
            "tags": self.tags,
        }


def _normalize_level(level: AlertLevel) -> str:
    """Return the string value of an AlertLevel."""
    return level.value if isinstance(level, AlertLevel) else str(level)


def normalize_status(
    status: PipelineStatus,
    default_message: str = "",
    extra_tags: Optional[List[str]] = None,
) -> NormalizedStatus:
    """Convert a PipelineStatus into a NormalizedStatus."""
    return NormalizedStatus(
        name=status.pipeline_name,
        level=_normalize_level(status.level),
        error_rate=round(status.error_rate or 0.0, 6),
        latency_ms=round(status.latency_ms or 0.0, 3),
        message=status.message or default_message,
        tags=list(extra_tags or []),
    )


def normalize_all(
    statuses: List[PipelineStatus],
    default_message: str = "",
    extra_tags: Optional[List[str]] = None,
) -> List[NormalizedStatus]:
    """Normalize a list of PipelineStatus objects."""
    return [
        normalize_status(s, default_message=default_message, extra_tags=extra_tags)
        for s in statuses
    ]
