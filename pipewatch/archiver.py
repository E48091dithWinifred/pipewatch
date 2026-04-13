"""Archive and prune old pipeline run history records."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from pipewatch.history import RunRecord


@dataclass
class ArchiveConfig:
    history_path: str
    archive_path: str
    max_age_days: int = 30
    dry_run: bool = False


@dataclass
class ArchiveResult:
    archived: int
    pruned: int
    kept: int
    archive_path: Optional[str]

    @property
    def summary(self) -> str:
        return (
            f"archived={self.archived} pruned={self.pruned} kept={self.kept}"
        )


def _cutoff_dt(max_age_days: int) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(days=max_age_days)


def _load_records(path: str) -> List[dict]:
    p = Path(path)
    if not p.exists():
        return []
    with p.open() as fh:
        return json.load(fh)


def _save_records(path: str, records: List[dict]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as fh:
        json.dump(records, fh, indent=2)


def archive_old_records(cfg: ArchiveConfig) -> ArchiveResult:
    """Move records older than max_age_days to archive_path and prune from history."""
    records = _load_records(cfg.history_path)
    cutoff = _cutoff_dt(cfg.max_age_days)

    keep: List[dict] = []
    to_archive: List[dict] = []

    for rec in records:
        ts_str = rec.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            keep.append(rec)
            continue

        if ts < cutoff:
            to_archive.append(rec)
        else:
            keep.append(rec)

    if not cfg.dry_run:
        existing_archive = _load_records(cfg.archive_path)
        _save_records(cfg.archive_path, existing_archive + to_archive)
        _save_records(cfg.history_path, keep)

    return ArchiveResult(
        archived=len(to_archive),
        pruned=len(to_archive),
        kept=len(keep),
        archive_path=cfg.archive_path if not cfg.dry_run else None,
    )
