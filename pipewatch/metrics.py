"""Metrics collection and aggregation for pipeline monitoring."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class PipelineMetrics:
    """Snapshot of metrics collected for a single pipeline run."""

    pipeline_name: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    records_processed: int = 0
    records_failed: int = 0
    duration_seconds: float = 0.0
    rows_per_second: Optional[float] = None

    def __post_init__(self) -> None:
        if self.rows_per_second is None and self.duration_seconds > 0:
            self.rows_per_second = self.records_processed / self.duration_seconds

    @property
    def error_rate(self) -> float:
        """Return fraction of failed records (0.0 – 1.0)."""
        total = self.records_processed + self.records_failed
        if total == 0:
            return 0.0
        return self.records_failed / total

    @property
    def latency_ms(self) -> float:
        """Return pipeline duration expressed in milliseconds."""
        return self.duration_seconds * 1000.0


@dataclass
class MetricsSummary:
    """Aggregated statistics over a collection of PipelineMetrics snapshots."""

    pipeline_name: str
    samples: List[PipelineMetrics] = field(default_factory=list)

    def add(self, metrics: PipelineMetrics) -> None:
        """Append a new metrics snapshot to the summary."""
        if metrics.pipeline_name != self.pipeline_name:
            raise ValueError(
                f"Metrics pipeline name '{metrics.pipeline_name}' does not match "
                f"summary pipeline name '{self.pipeline_name}'."
            )
        self.samples.append(metrics)

    @property
    def avg_error_rate(self) -> float:
        if not self.samples:
            return 0.0
        return sum(s.error_rate for s in self.samples) / len(self.samples)

    @property
    def avg_latency_ms(self) -> float:
        if not self.samples:
            return 0.0
        return sum(s.latency_ms for s in self.samples) / len(self.samples)

    @property
    def total_records_processed(self) -> int:
        return sum(s.records_processed for s in self.samples)

    @property
    def total_records_failed(self) -> int:
        return sum(s.records_failed for s in self.samples)
