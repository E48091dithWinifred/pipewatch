"""Audit log for pipeline status changes and alert events."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import List, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class AuditEntry:
    timestamp: str
    pipeline: str
    level: str
    message: str
    source: str = "pipewatch"

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class AuditConfig:
    path: str
    max_entries: int = 500


def _load_entries(path: str) -> List[dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r") as fh:
        try:
            return json.load(fh)
        except (json.JSONDecodeError, ValueError):
            return []


def _save_entries(path: str, entries: List[dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as fh:
        json.dump(entries, fh, indent=2)


def record_event(
    config: AuditConfig,
    pipeline: str,
    level: str,
    message: str,
    source: str = "pipewatch",
) -> AuditEntry:
    entry = AuditEntry(
        timestamp=_now_iso(),
        pipeline=pipeline,
        level=level,
        message=message,
        source=source,
    )
    entries = _load_entries(config.path)
    entries.append(entry.as_dict())
    if len(entries) > config.max_entries:
        entries = entries[-config.max_entries :]
    _save_entries(config.path, entries)
    return entry


def load_audit_log(config: AuditConfig) -> List[AuditEntry]:
    raw = _load_entries(config.path)
    return [
        AuditEntry(
            timestamp=r["timestamp"],
            pipeline=r["pipeline"],
            level=r["level"],
            message=r["message"],
            source=r.get("source", "pipewatch"),
        )
        for r in raw
    ]


def filter_audit_log(
    entries: List[AuditEntry],
    pipeline: Optional[str] = None,
    level: Optional[str] = None,
) -> List[AuditEntry]:
    result = entries
    if pipeline:
        result = [e for e in result if e.pipeline == pipeline]
    if level:
        result = [e for e in result if e.level.upper() == level.upper()]
    return result
