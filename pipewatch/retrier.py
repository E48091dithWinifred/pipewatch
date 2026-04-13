"""Retry tracking and policy enforcement for pipeline checks."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import json
import os
import time

from pipewatch.checker import AlertLevel


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 5.0
    retry_on: List[str] = field(default_factory=lambda: ["CRITICAL"])


@dataclass
class RetryEntry:
    pipeline: str
    level: str
    attempts: int
    last_attempt_ts: float
    resolved: bool = False

    def should_retry(self, policy: RetryPolicy, now: Optional[float] = None) -> bool:
        if self.resolved:
            return False
        if self.attempts >= policy.max_attempts:
            return False
        if self.level not in policy.retry_on:
            return False
        elapsed = (now or time.time()) - self.last_attempt_ts
        return elapsed >= policy.backoff_seconds

    def exhausted(self, policy: RetryPolicy) -> bool:
        return not self.resolved and self.attempts >= policy.max_attempts


def _state_path(state_file: str) -> str:
    return state_file


def _load_state(state_file: str) -> Dict[str, RetryEntry]:
    if not os.path.exists(state_file):
        return {}
    with open(state_file) as fh:
        raw = json.load(fh)
    return {
        k: RetryEntry(**v) for k, v in raw.items()
    }


def _save_state(state_file: str, state: Dict[str, RetryEntry]) -> None:
    os.makedirs(os.path.dirname(state_file) or ".", exist_ok=True)
    with open(state_file, "w") as fh:
        json.dump({k: v.__dict__ for k, v in state.items()}, fh, indent=2)


def record_attempt(
    pipeline: str,
    level: AlertLevel,
    state_file: str,
    now: Optional[float] = None,
) -> RetryEntry:
    """Record a retry attempt for a pipeline, persisting state."""
    state = _load_state(state_file)
    ts = now or time.time()
    level_str = level.value
    if pipeline in state:
        entry = state[pipeline]
        entry.attempts += 1
        entry.last_attempt_ts = ts
        entry.level = level_str
        entry.resolved = False
    else:
        entry = RetryEntry(
            pipeline=pipeline,
            level=level_str,
            attempts=1,
            last_attempt_ts=ts,
        )
        state[pipeline] = entry
    _save_state(state_file, state)
    return entry


def resolve(
    pipeline: str,
    state_file: str,
) -> Optional[RetryEntry]:
    """Mark a pipeline as resolved (no longer needs retrying)."""
    state = _load_state(state_file)
    if pipeline not in state:
        return None
    state[pipeline].resolved = True
    _save_state(state_file, state)
    return state[pipeline]


def get_pending(
    state_file: str,
    policy: RetryPolicy,
    now: Optional[float] = None,
) -> List[RetryEntry]:
    """Return entries that are eligible for retry."""
    state = _load_state(state_file)
    return [e for e in state.values() if e.should_retry(policy, now)]
