"""tee.py — Duplicate a list of statuses into multiple named output slots."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipewatch.checker import PipelineStatus


@dataclass
class TeeConfig:
    outputs: List[str] = field(default_factory=lambda: ["primary", "secondary"])

    def __post_init__(self) -> None:
        if len(self.outputs) < 1:
            raise ValueError("TeeConfig requires at least one output name")
        if len(self.outputs) != len(set(self.outputs)):
            raise ValueError("TeeConfig output names must be unique")


@dataclass
class TeeResult:
    slots: Dict[str, List[PipelineStatus]]

    @property
    def output_names(self) -> List[str]:
        return list(self.slots.keys())

    @property
    def total_slots(self) -> int:
        return len(self.slots)

    def get(self, name: str) -> List[PipelineStatus]:
        return self.slots.get(name, [])

    def summary(self) -> str:
        counts = ", ".join(
            f"{name}={len(items)}" for name, items in self.slots.items()
        )
        return f"TeeResult({counts})"


def tee_statuses(
    statuses: List[PipelineStatus],
    config: TeeConfig | None = None,
) -> TeeResult:
    """Copy statuses into each named output slot."""
    if config is None:
        config = TeeConfig()
    slots: Dict[str, List[PipelineStatus]] = {
        name: list(statuses) for name in config.outputs
    }
    return TeeResult(slots=slots)
