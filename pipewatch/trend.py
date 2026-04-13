"""Trend analysis for pipeline run history."""
from __future__ import annotations

from dataclasses import dataclass
from statistics import mean, stdev
from typing import List, Optional

from pipewatch.history import RunRecord


@dataclass
class TrendReport:
    pipeline_name: str
    sample_count: int
    avg_error_rate: float
    avg_latency_ms: float
    error_rate_stddev: float
    latency_stddev: float
    degrading: bool
    improving: bool

    def summary(self) -> str:
        direction = "degrading" if self.degrading else ("improving" if self.improving else "stable")
        return (
            f"{self.pipeline_name}: {self.sample_count} runs | "
            f"avg_error_rate={self.avg_error_rate:.2%} "
            f"avg_latency={self.avg_latency_ms:.1f}ms | trend={direction}"
        )


def _is_degrading(values: List[float]) -> bool:
    """Return True if the last third of values is higher than the first third."""
    if len(values) < 3:
        return False
    third = max(1, len(values) // 3)
    return mean(values[-third:]) > mean(values[:third])


def _is_improving(values: List[float]) -> bool:
    if len(values) < 3:
        return False
    third = max(1, len(values) // 3)
    return mean(values[-third:]) < mean(values[:third])


def analyze_trend(
    records: List[RunRecord],
    pipeline_name: str,
    window: int = 20,
) -> Optional[TrendReport]:
    """Compute trend statistics for a specific pipeline from run records."""
    relevant = [
        r for r in records if r.pipeline_name == pipeline_name
    ][-window:]

    if not relevant:
        return None

    error_rates = [r.error_rate for r in relevant]
    latencies = [r.latency_ms for r in relevant]

    return TrendReport(
        pipeline_name=pipeline_name,
        sample_count=len(relevant),
        avg_error_rate=mean(error_rates),
        avg_latency_ms=mean(latencies),
        error_rate_stddev=stdev(error_rates) if len(error_rates) > 1 else 0.0,
        latency_stddev=stdev(latencies) if len(latencies) > 1 else 0.0,
        degrading=_is_degrading(error_rates) or _is_degrading(latencies),
        improving=_is_improving(error_rates) and _is_improving(latencies),
    )
