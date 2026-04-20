"""projector.py — Project future pipeline health based on historical trends."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import RunRecord
from pipewatch.trend import analyze_trend


@dataclass
class ProjectionPoint:
    step: int
    projected_error_rate: float
    projected_latency_ms: float

    def summary(self) -> str:
        return (
            f"step={self.step} "
            f"error_rate={self.projected_error_rate:.4f} "
            f"latency_ms={self.projected_latency_ms:.1f}"
        )


@dataclass
class ProjectionResult:
    pipeline: str
    steps: int
    points: List[ProjectionPoint]
    degrading: bool
    improving: bool

    def summary(self) -> str:
        direction = "degrading" if self.degrading else ("improving" if self.improving else "stable")
        return f"{self.pipeline}: {direction} over {self.steps} projected steps"


def _linear_extrapolate(values: List[float], steps: int) -> List[float]:
    """Simple linear extrapolation from the last two values."""
    if len(values) < 2:
        last = values[-1] if values else 0.0
        return [last] * steps
    delta = (values[-1] - values[-2])
    last = values[-1]
    return [max(0.0, last + delta * (i + 1)) for i in range(steps)]


def project(
    pipeline: str,
    records: List[RunRecord],
    steps: int = 3,
) -> Optional[ProjectionResult]:
    """Project future error_rate and latency_ms for *pipeline* over *steps* intervals."""
    if not records:
        return None
    pipeline_records = [r for r in records if r.pipeline == pipeline]
    if len(pipeline_records) < 2:
        return None

    error_rates = [r.error_rate for r in pipeline_records]
    latencies = [r.latency_ms for r in pipeline_records]

    proj_errors = _linear_extrapolate(error_rates, steps)
    proj_latencies = _linear_extrapolate(latencies, steps)

    points = [
        ProjectionPoint(
            step=i + 1,
            projected_error_rate=proj_errors[i],
            projected_latency_ms=proj_latencies[i],
        )
        for i in range(steps)
    ]

    trend = analyze_trend(pipeline, pipeline_records)
    degrading = trend.degrading if trend else False
    improving = trend.improving if trend else False

    return ProjectionResult(
        pipeline=pipeline,
        steps=steps,
        points=points,
        degrading=degrading,
        improving=improving,
    )


def project_all(
    records: List[RunRecord],
    steps: int = 3,
) -> List[ProjectionResult]:
    """Project all pipelines present in *records*."""
    pipelines = list(dict.fromkeys(r.pipeline for r in records))
    results = []
    for name in pipelines:
        result = project(name, records, steps=steps)
        if result is not None:
            results.append(result)
    return results
