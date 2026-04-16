"""Pin pipelines to suppress status changes for a fixed duration."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pipewatch.checker import PipelineStatus


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class PinEntry:
    pipeline: str
    pinned_at: str
    expires_at: Optional[str]  # None = permanent
    reason: str = ""

    def is_active(self) -> bool:
        if self.expires_at is None:
            return True
        return datetime.fromisoformat(self.expires_at) > datetime.now(timezone.utc)


@dataclass
class PinConfig:
    state_path: str = ".pipewatch/pin_state.json"


def _load_pins(path: Path) -> List[PinEntry]:
    if not path.exists():
        return []
    data = json.loads(path.read_text())
    return [PinEntry(**e) for e in data]


def _save_pins(path: Path, entries: List[PinEntry]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([e.__dict__ for e in entries], indent=2))


def pin_pipeline(
    name: str,
    cfg: PinConfig,
    expires_at: Optional[str] = None,
    reason: str = "",
) -> PinEntry:
    path = Path(cfg.state_path)
    entries = _load_pins(path)
    entries = [e for e in entries if e.pipeline != name]
    entry = PinEntry(pipeline=name, pinned_at=_now_iso(), expires_at=expires_at, reason=reason)
    entries.append(entry)
    _save_pins(path, entries)
    return entry


def unpin_pipeline(name: str, cfg: PinConfig) -> bool:
    path = Path(cfg.state_path)
    entries = _load_pins(path)
    before = len(entries)
    entries = [e for e in entries if e.pipeline != name]
    _save_pins(path, entries)
    return len(entries) < before


def is_pinned(status: PipelineStatus, cfg: PinConfig) -> bool:
    path = Path(cfg.state_path)
    entries = _load_pins(path)
    return any(e.pipeline == status.pipeline_name and e.is_active() for e in entries)


def filter_pinned(
    statuses: List[PipelineStatus], cfg: PinConfig
) -> tuple[List[PipelineStatus], List[PipelineStatus]]:
    """Return (unpinned, pinned) partitions."""
    unpinned, pinned = [], []
    for s in statuses:
        (pinned if is_pinned(s, cfg) else unpinned).append(s)
    return unpinned, pinned
