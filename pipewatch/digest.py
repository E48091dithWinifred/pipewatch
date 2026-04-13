"""Digest module: generate periodic summary digests across all pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import RunRecord, get_recent
from pipewatch.trend import TrendReport, analyze_trend
from pipewatch.sparkline import labeled_sparkline


@dataclass
class PipelineDigest:
    pipeline_name: str
    total_runs: int
    critical_count: int
    warning_count: int
    ok_count: int
    avg_error_rate: float
    avg_latency_ms: float
    trend: Optional[TrendReport]
    sparkline: str

    @property
    def health_score(self) -> float:
        """Simple 0-100 score: penalise criticals heavily, warnings lightly."""
        if self.total_runs == 0:
            return 100.0
        penalty = (self.critical_count * 10 + self.warning_count * 3) / self.total_runs
        return max(0.0, round(100.0 - penalty * 100, 1))


@dataclass
class Digest:
    window_hours: int
    pipelines: List[PipelineDigest] = field(default_factory=list)

    @property
    def most_critical(self) -> Optional[PipelineDigest]:
        if not self.pipelines:
            return None
        return min(self.pipelines, key=lambda p: p.health_score)


def _avg(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def build_digest(pipeline_names: List[str], history_path: str, window_hours: int = 24) -> Digest:
    """Build a Digest for *pipeline_names* using runs within *window_hours*."""
    digest = Digest(window_hours=window_hours)

    for name in pipeline_names:
        records: List[RunRecord] = get_recent(history_path, name, window_hours)

        if not records:
            digest.pipelines.append(
                PipelineDigest(
                    pipeline_name=name,
                    total_runs=0,
                    critical_count=0,
                    warning_count=0,
                    ok_count=0,
                    avg_error_rate=0.0,
                    avg_latency_ms=0.0,
                    trend=None,
                    sparkline="",
                )
            )
            continue

        levels = [r.alert_level for r in records]
        error_rates = [r.error_rate for r in records]
        latencies = [r.latency_ms for r in records]

        trend = analyze_trend(history_path, name)
        spark = labeled_sparkline(name, error_rates)

        digest.pipelines.append(
            PipelineDigest(
                pipeline_name=name,
                total_runs=len(records),
                critical_count=levels.count("critical"),
                warning_count=levels.count("warning"),
                ok_count=levels.count("ok"),
                avg_error_rate=round(_avg(error_rates), 4),
                avg_latency_ms=round(_avg(latencies), 2),
                trend=trend,
                sparkline=spark,
            )
        )

    return digest
