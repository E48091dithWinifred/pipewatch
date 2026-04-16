"""windower.py — sliding window aggregation over pipeline status history."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.history import RunRecord
from pipewatch.checker import AlertLevel


@dataclass
class WindowConfig:
    window_size: int = 10  # number of most-recent records to consider

    def __post_init__(self) -> None:
        if self.window_size < 1:
            raise ValueError("window_size must be >= 1")


@dataclass
class WindowStats:
    pipeline: str
    window_size: int
    total_records: int
    avg_error_rate: float
    avg_latency_ms: float
    critical_count: int
    warning_count: int
    ok_count: int

    @property
    def dominant_level(self) -> str:
        if self.critical_count > 0:
            return AlertLevel.CRITICAL.value
        if self.warning_count > 0:
            return AlertLevel.WARNING.value
        return AlertLevel.OK.value

    def summary(self) -> str:
        return (
            f"{self.pipeline}: window={self.total_records} "
            f"avg_err={self.avg_error_rate:.3f} "
            f"avg_lat={self.avg_latency_ms:.1f}ms "
            f"dominant={self.dominant_level}"
        )


def _avg(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def compute_window(
    pipeline: str,
    records: List[RunRecord],
    config: Optional[WindowConfig] = None,
) -> Optional[WindowStats]:
    cfg = config or WindowConfig()
    pipeline_records = [r for r in records if r.pipeline == pipeline]
    if not pipeline_records:
        return None
    window = pipeline_records[-cfg.window_size :]
    error_rates = [r.error_rate for r in window]
    latencies = [r.latency_ms for r in window]
    levels = [r.level for r in window]
    return WindowStats(
        pipeline=pipeline,
        window_size=cfg.window_size,
        total_records=len(window),
        avg_error_rate=_avg(error_rates),
        avg_latency_ms=_avg(latencies),
        critical_count=levels.count(AlertLevel.CRITICAL.value),
        warning_count=levels.count(AlertLevel.WARNING.value),
        ok_count=levels.count(AlertLevel.OK.value),
    )


def compute_all_windows(
    records: List[RunRecord],
    config: Optional[WindowConfig] = None,
) -> List[WindowStats]:
    pipelines = list(dict.fromkeys(r.pipeline for r in records))
    results = []
    for name in pipelines:
        stats = compute_window(name, records, config)
        if stats is not None:
            results.append(stats)
    return results
