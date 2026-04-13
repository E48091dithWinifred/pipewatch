"""Pipeline run profiler: tracks timing and resource usage per pipeline run."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ProfileEntry:
    pipeline_name: str
    started_at: float
    ended_at: float
    duration_ms: float
    records_processed: int
    error_count: int

    @property
    def throughput(self) -> float:
        """Records processed per second."""
        seconds = self.duration_ms / 1000.0
        if seconds <= 0:
            return 0.0
        return self.records_processed / seconds


@dataclass
class ProfilerSession:
    entries: List[ProfileEntry] = field(default_factory=list)
    _start_times: Dict[str, float] = field(default_factory=dict, repr=False)

    def start(self, pipeline_name: str) -> None:
        """Mark the start of a pipeline run."""
        self._start_times[pipeline_name] = time.monotonic()

    def stop(
        self,
        pipeline_name: str,
        records_processed: int = 0,
        error_count: int = 0,
    ) -> Optional[ProfileEntry]:
        """Mark the end of a pipeline run and record the entry."""
        started_at = self._start_times.pop(pipeline_name, None)
        if started_at is None:
            return None
        ended_at = time.monotonic()
        duration_ms = (ended_at - started_at) * 1000.0
        entry = ProfileEntry(
            pipeline_name=pipeline_name,
            started_at=started_at,
            ended_at=ended_at,
            duration_ms=duration_ms,
            records_processed=records_processed,
            error_count=error_count,
        )
        self.entries.append(entry)
        return entry

    def get_entries(self, pipeline_name: str) -> List[ProfileEntry]:
        """Return all recorded entries for a given pipeline."""
        return [e for e in self.entries if e.pipeline_name == pipeline_name]

    def average_duration_ms(self, pipeline_name: str) -> Optional[float]:
        """Average duration in ms across all recorded runs for a pipeline."""
        entries = self.get_entries(pipeline_name)
        if not entries:
            return None
        return sum(e.duration_ms for e in entries) / len(entries)

    def slowest(self) -> Optional[ProfileEntry]:
        """Return the single slowest entry across all pipelines."""
        if not self.entries:
            return None
        return max(self.entries, key=lambda e: e.duration_ms)
