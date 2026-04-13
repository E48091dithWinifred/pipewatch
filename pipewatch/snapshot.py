"""Snapshot module: capture and compare pipeline metric snapshots."""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass
class MetricSnapshot:
    pipeline_name: str
    captured_at: str
    error_rate: float
    latency_ms: float
    rows_per_second: float
    alert_level: str


@dataclass
class SnapshotDiff:
    pipeline_name: str
    error_rate_delta: float
    latency_delta_ms: float
    rows_per_second_delta: float
    level_changed: bool
    previous_level: str
    current_level: str


def _snapshot_path(directory: str, pipeline_name: str) -> str:
    safe = pipeline_name.replace(" ", "_").replace("/", "-")
    return os.path.join(directory, f"{safe}.snapshot.json")


def save_snapshot(snapshot: MetricSnapshot, directory: str) -> None:
    os.makedirs(directory, exist_ok=True)
    path = _snapshot_path(directory, snapshot.pipeline_name)
    with open(path, "w") as fh:
        json.dump(asdict(snapshot), fh, indent=2)


def load_snapshot(pipeline_name: str, directory: str) -> Optional[MetricSnapshot]:
    path = _snapshot_path(directory, pipeline_name)
    if not os.path.exists(path):
        return None
    with open(path) as fh:
        data = json.load(fh)
    return MetricSnapshot(**data)


def diff_snapshots(previous: MetricSnapshot, current: MetricSnapshot) -> SnapshotDiff:
    return SnapshotDiff(
        pipeline_name=current.pipeline_name,
        error_rate_delta=round(current.error_rate - previous.error_rate, 6),
        latency_delta_ms=round(current.latency_ms - previous.latency_ms, 3),
        rows_per_second_delta=round(current.rows_per_second - previous.rows_per_second, 3),
        level_changed=current.alert_level != previous.alert_level,
        previous_level=previous.alert_level,
        current_level=current.alert_level,
    )


def make_snapshot(pipeline_name: str, error_rate: float, latency_ms: float,
                  rows_per_second: float, alert_level: str) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline_name=pipeline_name,
        captured_at=datetime.now(timezone.utc).isoformat(),
        error_rate=error_rate,
        latency_ms=latency_ms,
        rows_per_second=rows_per_second,
        alert_level=alert_level,
    )
