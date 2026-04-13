"""Alert throttling: suppress repeated alerts within a cooldown window."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

DEFAULT_COOLDOWN_MINUTES = 30


@dataclass
class ThrottleEntry:
    pipeline: str
    level: str
    last_fired: str  # ISO timestamp
    fire_count: int = 1


@dataclass
class ThrottleConfig:
    cooldown_minutes: int = DEFAULT_COOLDOWN_MINUTES
    state_path: str = ".pipewatch/throttle_state.json"


def _load_state(path: str) -> Dict[str, ThrottleEntry]:
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        raw = json.load(f)
    return {
        k: ThrottleEntry(**v) for k, v in raw.items()
    }


def _save_state(path: str, state: Dict[str, ThrottleEntry]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump({k: vars(v) for k, v in state.items()}, f, indent=2)


def _entry_key(pipeline: str, level: str) -> str:
    return f"{pipeline}::{level}"


def is_throttled(pipeline: str, level: str, config: ThrottleConfig) -> bool:
    """Return True if this alert should be suppressed due to cooldown."""
    state = _load_state(config.state_path)
    key = _entry_key(pipeline, level)
    if key not in state:
        return False
    entry = state[key]
    last = datetime.fromisoformat(entry.last_fired)
    cutoff = datetime.utcnow() - timedelta(minutes=config.cooldown_minutes)
    return last >= cutoff


def record_fire(pipeline: str, level: str, config: ThrottleConfig) -> ThrottleEntry:
    """Record that an alert fired; update state and return the entry."""
    state = _load_state(config.state_path)
    key = _entry_key(pipeline, level)
    now = datetime.utcnow().isoformat()
    if key in state:
        state[key].last_fired = now
        state[key].fire_count += 1
    else:
        state[key] = ThrottleEntry(pipeline=pipeline, level=level, last_fired=now)
    _save_state(config.state_path, state)
    return state[key]


def reset_throttle(pipeline: str, level: str, config: ThrottleConfig) -> bool:
    """Remove throttle entry for a pipeline+level. Returns True if removed."""
    state = _load_state(config.state_path)
    key = _entry_key(pipeline, level)
    if key in state:
        del state[key]
        _save_state(config.state_path, state)
        return True
    return False
