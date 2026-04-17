"""tailer.py — Tail the most recent N pipeline statuses from history."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.history import RunRecord, _load_records


@dataclass
class TailConfig:
    n: int = 10
    pipeline: Optional[str] = None

    def __post_init__(self) -> None:
        if self.n < 1:
            raise ValueError("n must be >= 1")


@dataclass
class TailResult:
    records: List[RunRecord]
    config: TailConfig

    @property
    def count(self) -> int:
        return len(self.records)

    def summary(self) -> str:
        name_part = f" for '{self.config.pipeline}'" if self.config.pipeline else ""
        return f"Showing {self.count} record(s){name_part}"


def tail_history(history_path: str, config: TailConfig) -> TailResult:
    """Load and return the last N records, optionally filtered by pipeline name."""
    records = _load_records(history_path)
    if config.pipeline:
        records = [r for r in records if r.pipeline == config.pipeline]
    tail = records[-config.n:]
    return TailResult(records=tail, config=config)
