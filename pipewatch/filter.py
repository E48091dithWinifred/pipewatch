"""Filter pipeline statuses by various criteria."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Sequence

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class FilterConfig:
    """Criteria used to narrow down a list of PipelineStatus results."""

    levels: List[str] = field(default_factory=list)
    """Only include statuses whose alert level is in this list (e.g. ['WARNING', 'CRITICAL'])."""

    name_contains: Optional[str] = None
    """Only include pipelines whose name contains this substring (case-insensitive)."""

    max_error_rate: Optional[float] = None
    """Only include pipelines with error_rate <= this value."""

    min_error_rate: Optional[float] = None
    """Only include pipelines with error_rate >= this value."""


def _level_matches(status: PipelineStatus, levels: List[str]) -> bool:
    """Return True when *levels* is empty or the status level is in the list."""
    if not levels:
        return True
    return status.level.name in [lv.upper() for lv in levels]


def _name_matches(status: PipelineStatus, name_contains: Optional[str]) -> bool:
    if name_contains is None:
        return True
    return name_contains.lower() in status.pipeline_name.lower()


def _error_rate_matches(
    status: PipelineStatus,
    min_rate: Optional[float],
    max_rate: Optional[float],
) -> bool:
    rate = status.error_rate
    if rate is None:
        return True
    if min_rate is not None and rate < min_rate:
        return False
    if max_rate is not None and rate > max_rate:
        return False
    return True


def apply_filter(
    statuses: Sequence[PipelineStatus],
    cfg: FilterConfig,
) -> List[PipelineStatus]:
    """Return the subset of *statuses* that match every criterion in *cfg*."""
    result: List[PipelineStatus] = []
    for s in statuses:
        if not _level_matches(s, cfg.levels):
            continue
        if not _name_matches(s, cfg.name_contains):
            continue
        if not _error_rate_matches(s, cfg.min_error_rate, cfg.max_error_rate):
            continue
        result.append(s)
    return result
