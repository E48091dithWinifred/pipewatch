"""Anomaly detection for pipeline metrics using simple statistical methods."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import RunRecord


@dataclass
class AnomalyResult:
    pipeline_name: str
    metric: str          # 'error_rate' | 'latency_ms'
    current_value: float
    mean: float
    std_dev: float
    z_score: float
    is_anomaly: bool

    @property
    def summary(self) -> str:
        direction = "high" if self.current_value > self.mean else "low"
        return (
            f"{self.pipeline_name}/{self.metric} is anomalously {direction} "
            f"(z={self.z_score:.2f}, current={self.current_value:.4f}, "
            f"mean={self.mean:.4f})"
        )


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std_dev(values: List[float], mean: float) -> float:
    if len(values) < 2:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return variance ** 0.5


def _z_score(value: float, mean: float, std: float) -> float:
    if std == 0.0:
        return 0.0
    return (value - mean) / std


def detect_anomaly(
    pipeline_name: str,
    records: List[RunRecord],
    metric: str,
    threshold_z: float = 2.5,
) -> Optional[AnomalyResult]:
    """Return AnomalyResult if the latest record deviates beyond threshold_z."""
    pipeline_records = [
        r for r in records if r.pipeline_name == pipeline_name
    ]
    if len(pipeline_records) < 3:
        return None

    values = [getattr(r, metric) for r in pipeline_records if getattr(r, metric) is not None]
    if len(values) < 3:
        return None

    history_values = values[:-1]
    current = values[-1]
    mean = _mean(history_values)
    std = _std_dev(history_values, mean)
    z = _z_score(current, mean, std)

    return AnomalyResult(
        pipeline_name=pipeline_name,
        metric=metric,
        current_value=current,
        mean=mean,
        std_dev=std,
        z_score=z,
        is_anomaly=abs(z) >= threshold_z,
    )


def detect_all_anomalies(
    records: List[RunRecord],
    threshold_z: float = 2.5,
) -> List[AnomalyResult]:
    """Run anomaly detection for every pipeline and both metrics."""
    names = list(dict.fromkeys(r.pipeline_name for r in records))
    results: List[AnomalyResult] = []
    for name in names:
        for metric in ("error_rate", "latency_ms"):
            result = detect_anomaly(name, records, metric, threshold_z)
            if result is not None and result.is_anomaly:
                results.append(result)
    return results
