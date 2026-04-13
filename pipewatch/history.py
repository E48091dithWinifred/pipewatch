"""Persistence layer for pipeline run history using a simple JSON store."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

DEFAULT_HISTORY_PATH = Path.home() / ".pipewatch" / "history.json"


@dataclass
class RunRecord:
    pipeline_name: str
    timestamp: str  # ISO-8601
    alert_level: str  # "OK", "WARNING", "CRITICAL"
    error_rate: float
    latency_ms: float
    rows_per_second: float
    message: Optional[str] = None

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()


def _load_records(path: Path) -> List[dict]:
    if not path.exists():
        return []
    with path.open("r") as fh:
        try:
            return json.load(fh)
        except json.JSONDecodeError:
            return []


def _save_records(records: List[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(records, fh, indent=2)


def append_run(record: RunRecord, path: Path = DEFAULT_HISTORY_PATH) -> None:
    """Append a single run record to the history file."""
    records = _load_records(path)
    records.append(asdict(record))
    _save_records(records, path)


def load_history(
    pipeline_name: Optional[str] = None,
    path: Path = DEFAULT_HISTORY_PATH,
    limit: int = 100,
) -> List[RunRecord]:
    """Load run history, optionally filtered by pipeline name."""
    raw = _load_records(path)
    if pipeline_name:
        raw = [r for r in raw if r.get("pipeline_name") == pipeline_name]
    return [RunRecord(**r) for r in raw[-limit:]]


def clear_history(path: Path = DEFAULT_HISTORY_PATH) -> None:
    """Remove all stored history records."""
    if path.exists():
        path.unlink()
