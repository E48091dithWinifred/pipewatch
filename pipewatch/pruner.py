"""pruner.py — Remove low-priority or stale pipeline statuses based on configurable rules."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class PruneConfig:
    max_ok_count: Optional[int] = None          # keep at most N OK pipelines
    remove_levels: List[str] = field(default_factory=list)  # e.g. ["ok"]
    max_error_rate: Optional[float] = None      # drop pipelines below this error rate
    name_prefix_exclude: Optional[str] = None  # drop pipelines whose name starts with prefix


@dataclass
class PruneResult:
    kept: List[PipelineStatus]
    pruned: List[PipelineStatus]

    @property
    def pruned_count(self) -> int:
        return len(self.pruned)

    @property
    def kept_count(self) -> int:
        return len(self.kept)

    def summary(self) -> str:
        return f"kept={self.kept_count} pruned={self.pruned_count}"


def _should_prune(status: PipelineStatus, config: PruneConfig, ok_seen: list) -> bool:
    if config.name_prefix_exclude and status.pipeline.startswith(config.name_prefix_exclude):
        return True

    level_name = status.level.name if isinstance(status.level, AlertLevel) else str(status.level)
    if level_name.lower() in [lvl.lower() for lvl in config.remove_levels]:
        if level_name.lower() == "ok":
            ok_seen.append(status)
            if config.max_ok_count is not None and len(ok_seen) > config.max_ok_count:
                return True
        else:
            return True

    if config.max_error_rate is not None and status.error_rate < config.max_error_rate:
        return True

    return False


def prune(statuses: List[PipelineStatus], config: PruneConfig) -> PruneResult:
    """Filter out pipeline statuses that match pruning criteria."""
    kept: List[PipelineStatus] = []
    pruned: List[PipelineStatus] = []
    ok_seen: list = []

    for status in statuses:
        if _should_prune(status, config, ok_seen):
            pruned.append(status)
        else:
            kept.append(status)

    return PruneResult(kept=kept, pruned=pruned)
