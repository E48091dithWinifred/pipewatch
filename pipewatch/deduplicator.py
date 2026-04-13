"""Deduplicator: suppress repeated alerts for the same pipeline/level combination."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class DedupeEntry:
    pipeline: str
    level: str
    count: int = 1
    last_seen: str = ""


@dataclass
class DedupeConfig:
    state_path: str = ".pipewatch_dedupe.json"
    min_repeat: int = 3  # fire alert only after this many consecutive identical hits


def _load_state(path: str) -> Dict[str, DedupeEntry]:
    if not os.path.exists(path):
        return {}
    with open(path) as fh:
        raw = json.load(fh)
    return {
        k: DedupeEntry(**v) for k, v in raw.items()
    }


def _save_state(path: str, state: Dict[str, DedupeEntry]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as fh:
        json.dump({k: vars(v) for k, v in state.items()}, fh, indent=2)


def _entry_key(status: PipelineStatus) -> str:
    return f"{status.pipeline}::{status.level.value}"


def should_suppress(status: PipelineStatus, config: DedupeConfig) -> bool:
    """Return True if this alert should be suppressed (not enough repeats yet)."""
    if status.level == AlertLevel.OK:
        return False
    state = _load_state(config.state_path)
    key = _entry_key(status)
    entry = state.get(key)
    if entry is None:
        return True  # first occurrence — suppress until threshold met
    return entry.count < config.min_repeat


def record_status(status: PipelineStatus, config: DedupeConfig, timestamp: str = "") -> DedupeEntry:
    """Update dedupe state for the given status and return the updated entry."""
    from pipewatch.history import now_iso

    ts = timestamp or now_iso()
    state = _load_state(config.state_path)
    key = _entry_key(status)

    if status.level == AlertLevel.OK:
        state.pop(key, None)
        _save_state(config.state_path, state)
        return DedupeEntry(pipeline=status.pipeline, level=status.level.value, count=0, last_seen=ts)

    if key in state:
        state[key].count += 1
        state[key].last_seen = ts
    else:
        state[key] = DedupeEntry(pipeline=status.pipeline, level=status.level.value, count=1, last_seen=ts)

    _save_state(config.state_path, state)
    return state[key]


def filter_statuses(
    statuses: List[PipelineStatus],
    config: DedupeConfig,
    timestamp: str = "",
) -> List[PipelineStatus]:
    """Record all statuses and return only those that should fire an alert."""
    result = []
    for s in statuses:
        entry = record_status(s, config, timestamp)
        if s.level == AlertLevel.OK or entry.count >= config.min_repeat:
            result.append(s)
    return result
