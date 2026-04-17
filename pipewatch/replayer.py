"""Replay historical pipeline runs for debugging or simulation."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Callable
from pipewatch.history import RunRecord, _load_records


@dataclass
class ReplayConfig:
    history_file: str
    pipeline_name: Optional[str] = None
    max_runs: int = 50
    reverse: bool = False


@dataclass
class ReplayResult:
    records: List[RunRecord]
    pipeline_name: Optional[str]
    total_replayed: int

    def summary(self) -> str:
        name = self.pipeline_name or "all pipelines"
        return f"Replayed {self.total_replayed} run(s) for {name}"


def load_replay_records(config: ReplayConfig) -> List[RunRecord]:
    records = _load_records(config.history_file)
    if config.pipeline_name:
        records = [r for r in records if r.pipeline_name == config.pipeline_name]
    records = records[-config.max_runs :]
    if config.reverse:
        records = list(reversed(records))
    return records


def replay(
    config: ReplayConfig,
    on_record: Optional[Callable[[RunRecord], None]] = None,
) -> ReplayResult:
    records = load_replay_records(config)
    if on_record:
        for record in records:
            on_record(record)
    return ReplayResult(
        records=records,
        pipeline_name=config.pipeline_name,
        total_replayed=len(records),
    )
