"""renamer.py – rename pipeline statuses via a mapping config."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from pipewatch.checker import PipelineStatus, AlertLevel


@dataclass
class RenameConfig:
    mapping: Dict[str, str]  # old_name -> new_name

    def get_new_name(self, name: str) -> str:
        return self.mapping.get(name, name)


@dataclass
class RenameResult:
    original: List[PipelineStatus]
    renamed: List[PipelineStatus]

    @property
    def changed_count(self) -> int:
        return sum(
            1
            for o, r in zip(self.original, self.renamed)
            if o.pipeline_name != r.pipeline_name
        )

    def summary(self) -> str:
        return (
            f"Renamed {self.changed_count} of {len(self.original)} pipelines."
        )


def _rename_status(status: PipelineStatus, new_name: str) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=new_name,
        level=status.level,
        message=status.message,
        error_rate=status.error_rate,
        latency_ms=status.latency_ms,
        checked_at=status.checked_at,
    )


def rename_statuses(
    statuses: List[PipelineStatus],
    config: RenameConfig,
) -> RenameResult:
    renamed = [
        _rename_status(s, config.get_new_name(s.pipeline_name))
        for s in statuses
    ]
    return RenameResult(original=statuses, renamed=renamed)
