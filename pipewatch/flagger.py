"""flagger.py — Mark pipeline statuses with boolean flags based on conditions."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.checker import PipelineStatus, AlertLevel


@dataclass
class FlagConfig:
    flag_stale: bool = True
    stale_threshold_seconds: float = 300.0
    flag_high_error_rate: bool = True
    error_rate_threshold: float = 0.05
    flag_slow: bool = True
    latency_threshold_ms: float = 5000.0
    flag_critical: bool = True


@dataclass
class FlaggedStatus:
    status: PipelineStatus
    flags: List[str] = field(default_factory=list)

    @property
    def name(self) -> str:
        return self.status.pipeline_name

    @property
    def level(self) -> AlertLevel:
        return self.status.level

    @property
    def is_flagged(self) -> bool:
        return len(self.flags) > 0

    def summary(self) -> str:
        if not self.flags:
            return f"{self.name}: no flags"
        return f"{self.name}: [{', '.join(self.flags)}]"


def _age_seconds(status: PipelineStatus) -> float:
    import datetime
    ts = getattr(status, "checked_at", None)
    if not ts:
        return 0.0
    try:
        checked = datetime.datetime.fromisoformat(ts)
        now = datetime.datetime.utcnow()
        return (now - checked).total_seconds()
    except Exception:
        return 0.0


def flag_status(status: PipelineStatus, config: Optional[FlagConfig] = None) -> FlaggedStatus:
    cfg = config or FlagConfig()
    flags: List[str] = []

    if cfg.flag_critical and status.level == AlertLevel.CRITICAL:
        flags.append("critical")

    if cfg.flag_high_error_rate:
        er = getattr(status, "error_rate", None)
        if er is not None and er >= cfg.error_rate_threshold:
            flags.append("high_error_rate")

    if cfg.flag_slow:
        lat = getattr(status, "latency_ms", None)
        if lat is not None and lat >= cfg.latency_threshold_ms:
            flags.append("slow")

    if cfg.flag_stale:
        age = _age_seconds(status)
        if age >= cfg.stale_threshold_seconds:
            flags.append("stale")

    return FlaggedStatus(status=status, flags=flags)


def flag_all(statuses: List[PipelineStatus], config: Optional[FlagConfig] = None) -> List[FlaggedStatus]:
    return [flag_status(s, config) for s in statuses]
