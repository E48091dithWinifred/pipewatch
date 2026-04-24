"""hedger.py — Hedge detection for pipelines running near threshold boundaries."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class HedgeResult:
    """A pipeline status that is close to crossing an alert threshold."""

    name: str
    level: AlertLevel
    error_rate: float
    latency_ms: float
    error_rate_margin: Optional[float]  # distance to next threshold, None if already critical
    latency_margin: Optional[float]
    is_hedging: bool

    def summary(self) -> str:
        parts = [f"{self.name} [{self.level.value}]"]
        if self.error_rate_margin is not None:
            parts.append(f"err_margin={self.error_rate_margin:.4f}")
        if self.latency_margin is not None:
            parts.append(f"lat_margin={self.latency_margin:.1f}ms")
        parts.append("HEDGING" if self.is_hedging else "stable")
        return " ".join(parts)


@dataclass
class HedgeConfig:
    """Configuration controlling how close to a threshold triggers a hedge flag."""

    error_rate_margin: float = 0.02   # flag if within 2 pp of next threshold
    latency_margin_ms: float = 50.0   # flag if within 50 ms of next threshold
    warning_error_rate: float = 0.05
    critical_error_rate: float = 0.10
    warning_latency_ms: float = 500.0
    critical_latency_ms: float = 1000.0


def _error_rate_margin(status: PipelineStatus, cfg: HedgeConfig) -> Optional[float]:
    """Return distance to the next error-rate threshold, or None if already critical."""
    er = status.error_rate
    if status.level == AlertLevel.CRITICAL:
        return None
    if status.level == AlertLevel.WARNING:
        return max(0.0, cfg.critical_error_rate - er)
    return max(0.0, cfg.warning_error_rate - er)


def _latency_margin(status: PipelineStatus, cfg: HedgeConfig) -> Optional[float]:
    """Return distance to the next latency threshold, or None if already critical."""
    lat = status.latency_ms
    if status.level == AlertLevel.CRITICAL:
        return None
    if status.level == AlertLevel.WARNING:
        return max(0.0, cfg.critical_latency_ms - lat)
    return max(0.0, cfg.warning_latency_ms - lat)


def hedge_status(status: PipelineStatus, cfg: Optional[HedgeConfig] = None) -> HedgeResult:
    """Evaluate whether a pipeline is hedging near a threshold boundary."""
    if cfg is None:
        cfg = HedgeConfig()

    er_margin = _error_rate_margin(status, cfg)
    lat_margin = _latency_margin(status, cfg)

    hedging = False
    if er_margin is not None and er_margin <= cfg.error_rate_margin:
        hedging = True
    if lat_margin is not None and lat_margin <= cfg.latency_margin_ms:
        hedging = True

    return HedgeResult(
        name=status.pipeline_name,
        level=status.level,
        error_rate=status.error_rate,
        latency_ms=status.latency_ms,
        error_rate_margin=er_margin,
        latency_margin=lat_margin,
        is_hedging=hedging,
    )


def hedge_all(
    statuses: List[PipelineStatus],
    cfg: Optional[HedgeConfig] = None,
) -> List[HedgeResult]:
    """Evaluate hedge status for a list of pipelines."""
    return [hedge_status(s, cfg) for s in statuses]
