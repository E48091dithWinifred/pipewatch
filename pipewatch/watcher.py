"""Watcher: orchestrate repeated pipeline checks and snapshot comparisons."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.snapshot import SnapshotDiff, diff_snapshots, load_snapshot, make_snapshot, save_snapshot


@dataclass
class WatchEvent:
    pipeline_name: str
    status: PipelineStatus
    diff: Optional[SnapshotDiff] = None
    is_first_run: bool = False


@dataclass
class WatcherConfig:
    snapshot_dir: str = ".pipewatch/snapshots"
    interval_seconds: float = 60.0
    on_event: Optional[Callable[[WatchEvent], None]] = None


def process_pipeline_status(
    status: PipelineStatus,
    config: WatcherConfig,
) -> WatchEvent:
    """Persist a snapshot and compute diff against the previous run."""
    previous = load_snapshot(status.pipeline_name, config.snapshot_dir)

    current_snap = make_snapshot(
        pipeline_name=status.pipeline_name,
        error_rate=status.error_rate,
        latency_ms=status.latency_ms,
        rows_per_second=status.rows_per_second,
        alert_level=status.level.name,
    )
    save_snapshot(current_snap, config.snapshot_dir)

    diff: Optional[SnapshotDiff] = None
    is_first = previous is None
    if previous is not None:
        diff = diff_snapshots(previous, current_snap)

    event = WatchEvent(
        pipeline_name=status.pipeline_name,
        status=status,
        diff=diff,
        is_first_run=is_first,
    )

    if config.on_event:
        config.on_event(event)

    return event


def watch_once(
    statuses: List[PipelineStatus],
    config: WatcherConfig,
) -> List[WatchEvent]:
    """Process a single batch of pipeline statuses."""
    return [process_pipeline_status(s, config) for s in statuses]
