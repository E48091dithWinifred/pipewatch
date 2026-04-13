"""Baseline management: capture and compare pipeline metric baselines."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import json
import os

from pipewatch.checker import PipelineStatus


@dataclass
class BaselineEntry:
    pipeline_name: str
    avg_error_rate: float
    avg_latency_ms: float
    sample_count: int


@dataclass
class BaselineDrift:
    pipeline_name: str
    error_rate_delta: float   # positive = worse
    latency_delta_ms: float   # positive = worse

    @property
    def has_drift(self) -> bool:
        return abs(self.error_rate_delta) > 0.001 or abs(self.latency_delta_ms) > 1.0


@dataclass
class BaselineReport:
    drifts: List[BaselineDrift] = field(default_factory=list)

    @property
    def degraded(self) -> List[BaselineDrift]:
        return [d for d in self.drifts if d.error_rate_delta > 0 or d.latency_delta_ms > 0]


def _baseline_path(directory: str, tag: str) -> str:
    return os.path.join(directory, f"baseline_{tag}.json")


def capture_baseline(statuses: List[PipelineStatus], directory: str, tag: str = "default") -> Dict[str, BaselineEntry]:
    """Compute per-pipeline averages and persist them as a baseline."""
    entries: Dict[str, BaselineEntry] = {}
    for s in statuses:
        entries[s.pipeline_name] = BaselineEntry(
            pipeline_name=s.pipeline_name,
            avg_error_rate=s.error_rate,
            avg_latency_ms=s.latency_ms,
            sample_count=1,
        )
    os.makedirs(directory, exist_ok=True)
    path = _baseline_path(directory, tag)
    with open(path, "w") as fh:
        json.dump({k: vars(v) for k, v in entries.items()}, fh, indent=2)
    return entries


def load_baseline(directory: str, tag: str = "default") -> Optional[Dict[str, BaselineEntry]]:
    """Load a previously saved baseline; returns None if not found."""
    path = _baseline_path(directory, tag)
    if not os.path.exists(path):
        return None
    with open(path) as fh:
        raw = json.load(fh)
    return {k: BaselineEntry(**v) for k, v in raw.items()}


def compare_to_baseline(
    statuses: List[PipelineStatus],
    baseline: Dict[str, BaselineEntry],
) -> BaselineReport:
    """Compare current statuses against a baseline and return drift report."""
    drifts: List[BaselineDrift] = []
    for s in statuses:
        entry = baseline.get(s.pipeline_name)
        if entry is None:
            continue
        drifts.append(BaselineDrift(
            pipeline_name=s.pipeline_name,
            error_rate_delta=s.error_rate - entry.avg_error_rate,
            latency_delta_ms=s.latency_ms - entry.avg_latency_ms,
        ))
    return BaselineReport(drifts=drifts)
