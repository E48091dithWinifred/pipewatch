"""Silencer: suppress alerts for pipelines matching configured rules."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class SilenceRule:
    """A rule that suppresses alerts for a pipeline within a time window."""

    pipeline_name: str
    reason: str
    until: Optional[str] = None  # ISO-8601 datetime; None means indefinite
    levels: List[str] = field(default_factory=list)  # empty = all levels

    def is_active(self) -> bool:
        """Return True if this rule is currently active."""
        if self.until is None:
            return True
        try:
            expiry = datetime.fromisoformat(self.until)
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            return datetime.now(tz=timezone.utc) < expiry
        except ValueError:
            return False

    def matches(self, status: PipelineStatus) -> bool:
        """Return True if this rule silences the given pipeline status."""
        if not self.is_active():
            return False
        if self.pipeline_name not in (status.pipeline_name, "*"):
            return False
        if self.levels:
            return status.level.value in self.levels
        return True


@dataclass
class SilencerConfig:
    rules: List[SilenceRule] = field(default_factory=list)


def is_silenced(status: PipelineStatus, config: SilencerConfig) -> bool:
    """Return True if any active rule silences this pipeline status."""
    return any(rule.matches(status) for rule in config.rules)


def apply_silencer(
    statuses: List[PipelineStatus], config: SilencerConfig
) -> tuple[List[PipelineStatus], List[PipelineStatus]]:
    """Split statuses into (active, silenced) lists."""
    active: List[PipelineStatus] = []
    silenced: List[PipelineStatus] = []
    for s in statuses:
        (silenced if is_silenced(s, config) else active).append(s)
    return active, silenced
