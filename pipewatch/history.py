"""Persistence layer for pipeline run records."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import List


@dataclass
class RunRecord:
    pipeline_name: str
    timestamp: str
    alert_level: str  # "ok" | "warning" | "critical"
    error_rate: float
    latency_ms: float
    rows_processed: int
    message: str = ""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_records(path: str) -> List[dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as fh:
        try:
            return json.load(fh)
        except json.JSONDecodeError:
            return []


def _save_records(path: str, records: List[dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(records, fh, indent=2)


def append_run(path: str, record: RunRecord) -> None:
    """Append a RunRecord to the JSON history file at *path*."""
    records = _load_records(path)
    records.append(asdict(record))
    _save_records(path, records)


def get_recent(path: str, pipeline_name: str, window_hours: int = 24) -> List[RunRecord]:
    """Return records for *pipeline_name* within the last *window_hours* hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    raw = _load_records(path)
    results: List[RunRecord] = []
    for entry in raw:
        if entry.get("pipeline_name") != pipeline_name:
            continue
        try:
            ts = datetime.fromisoformat(entry["timestamp"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except (KeyError, ValueError):
            continue
        if ts >= cutoff:
            results.append(RunRecord(**entry))
    return results


def load_history(path: str, pipeline_name: str) -> List[RunRecord]:
    """Return all records for *pipeline_name* regardless of age."""
    raw = _load_records(path)
    return [
        RunRecord(**entry)
        for entry in raw
        if entry.get("pipeline_name") == pipeline_name
    ]
