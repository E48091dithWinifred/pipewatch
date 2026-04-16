"""Rate limiter for pipeline alert dispatching."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List
import time


@dataclass
class LimiterConfig:
    max_per_window: int = 5
    window_seconds: int = 60

    def __post_init__(self) -> None:
        if self.max_per_window < 1:
            raise ValueError("max_per_window must be >= 1")
        if self.window_seconds < 1:
            raise ValueError("window_seconds must be >= 1")


@dataclass
class LimiterState:
    _timestamps: Dict[str, List[float]] = field(default_factory=dict)

    def _prune(self, key: str, now: float, window: int) -> None:
        cutoff = now - window
        self._timestamps[key] = [
            t for t in self._timestamps.get(key, []) if t > cutoff
        ]

    def is_allowed(self, key: str, config: LimiterConfig, now: float | None = None) -> bool:
        now = now if now is not None else time.monotonic()
        self._prune(key, now, config.window_seconds)
        return len(self._timestamps.get(key, [])) < config.max_per_window

    def record(self, key: str, now: float | None = None) -> None:
        now = now if now is not None else time.monotonic()
        self._timestamps.setdefault(key, []).append(now)

    def count_in_window(self, key: str, config: LimiterConfig, now: float | None = None) -> int:
        now = now if now is not None else time.monotonic()
        self._prune(key, now, config.window_seconds)
        return len(self._timestamps.get(key, []))


@dataclass
class LimitResult:
    key: str
    allowed: bool
    count_in_window: int
    max_per_window: int

    def summary(self) -> str:
        status = "allowed" if self.allowed else "blocked"
        return f"{self.key}: {status} ({self.count_in_window}/{self.max_per_window})"


def check_limit(
    key: str,
    state: LimiterState,
    config: LimiterConfig,
    now: float | None = None,
) -> LimitResult:
    now = now if now is not None else time.monotonic()
    allowed = state.is_allowed(key, config, now)
    if allowed:
        state.record(key, now)
    count = state.count_in_window(key, config, now)
    return LimitResult(
        key=key,
        allowed=allowed,
        count_in_window=count,
        max_per_window=config.max_per_window,
    )
