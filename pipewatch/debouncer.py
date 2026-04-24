"""Debouncer: suppress repeated alerts until a cooldown period has elapsed."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from pipewatch.checker import PipelineStatus


@dataclass
class DebounceConfig:
    cooldown_seconds: int = 300
    state_path: str = ".pipewatch/debounce_state.json"

    def __post_init__(self) -> None:
        if self.cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be non-negative")


@dataclass
class DebounceEntry:
    pipeline: str
    level: str
    last_fired: float  # epoch seconds
    fire_count: int = 1


@dataclass
class DebounceResult:
    allowed: List[PipelineStatus] = field(default_factory=list)
    suppressed: List[PipelineStatus] = field(default_factory=list)

    @property
    def allowed_count(self) -> int:
        return len(self.allowed)

    @property
    def suppressed_count(self) -> int:
        return len(self.suppressed)

    def summary(self) -> str:
        return (
            f"debounce: {self.allowed_count} allowed, "
            f"{self.suppressed_count} suppressed"
        )


def _load_state(path: str) -> Dict[str, DebounceEntry]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text())
        return {
            k: DebounceEntry(**v)
            for k, v in raw.items()
        }
    except Exception:
        return {}


def _save_state(path: str, state: Dict[str, DebounceEntry]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps(
            {k: vars(v) for k, v in state.items()},
            indent=2,
        )
    )


def _entry_key(status: PipelineStatus) -> str:
    return f"{status.pipeline_name}:{status.level.value}"


def debounce(
    statuses: List[PipelineStatus],
    config: Optional[DebounceConfig] = None,
    _now: Optional[float] = None,
) -> DebounceResult:
    """Allow a status through only if the cooldown has elapsed since last fire."""
    if config is None:
        config = DebounceConfig()

    now = _now if _now is not None else time.time()
    state = _load_state(config.state_path)
    result = DebounceResult()

    for status in statuses:
        from pipewatch.checker import AlertLevel
        if status.level == AlertLevel.OK:
            result.allowed.append(status)
            continue

        key = _entry_key(status)
        entry = state.get(key)

        if entry is None or (now - entry.last_fired) >= config.cooldown_seconds:
            result.allowed.append(status)
            state[key] = DebounceEntry(
                pipeline=status.pipeline_name,
                level=status.level.value,
                last_fired=now,
                fire_count=(entry.fire_count + 1 if entry else 1),
            )
        else:
            result.suppressed.append(status)

    _save_state(config.state_path, state)
    return result
