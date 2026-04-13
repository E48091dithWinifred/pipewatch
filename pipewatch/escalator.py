"""Escalation logic: promote alert level if a pipeline stays unhealthy
for more than a configurable number of consecutive runs."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class EscalationRule:
    """Escalate from *from_level* to *to_level* after *after_runs* consecutive hits."""
    from_level: str          # e.g. "WARNING"
    to_level: str            # e.g. "CRITICAL"
    after_runs: int = 3


@dataclass
class EscalationConfig:
    rules: List[EscalationRule] = field(default_factory=list)
    state_file: Optional[str] = None  # optional path for persisting counts


@dataclass
class EscalationState:
    """Tracks consecutive-run counts per pipeline."""
    counts: Dict[str, int] = field(default_factory=dict)

    def increment(self, name: str) -> int:
        self.counts[name] = self.counts.get(name, 0) + 1
        return self.counts[name]

    def reset(self, name: str) -> None:
        self.counts.pop(name, None)

    def get(self, name: str) -> int:
        return self.counts.get(name, 0)


def _find_rule(
    config: EscalationConfig, level: AlertLevel
) -> Optional[EscalationRule]:
    """Return the first matching rule for *level*, or None."""
    level_name = level.name  # "OK", "WARNING", "CRITICAL"
    for rule in config.rules:
        if rule.from_level.upper() == level_name:
            return rule
    return None


def escalate(
    status: PipelineStatus,
    config: EscalationConfig,
    state: EscalationState,
) -> PipelineStatus:
    """Return a (possibly escalated) copy of *status* and update *state*."""
    rule = _find_rule(config, status.level)

    if rule is None:
        # Not a level we track — reset any stored count
        state.reset(status.pipeline)
        return status

    count = state.increment(status.pipeline)

    if count >= rule.after_runs:
        try:
            new_level = AlertLevel[rule.to_level.upper()]
        except KeyError:
            return status
        message = (
            f"{status.message or ''} [escalated after {count} consecutive "
            f"{rule.from_level} runs]".strip()
        )
        return PipelineStatus(
            pipeline=status.pipeline,
            level=new_level,
            message=message,
            error_rate=status.error_rate,
            latency_ms=status.latency_ms,
        )

    return status
