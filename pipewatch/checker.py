"""Pipeline health checker module for evaluating metrics against alert thresholds."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from pipewatch.config import PipelineConfig


class AlertLevel(Enum):
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class PipelineStatus:
    pipeline_name: str
    alert_level: AlertLevel
    message: str
    error_rate: Optional[float] = None
    latency_seconds: Optional[float] = None
    rows_processed: Optional[int] = None

    @property
    def is_healthy(self) -> bool:
        return self.alert_level == AlertLevel.OK


def check_pipeline(pipeline: PipelineConfig, error_rate: float, latency_seconds: float, rows_processed: int) -> PipelineStatus:
    """Evaluate pipeline metrics against configured thresholds and return a status."""
    thresholds = pipeline.thresholds
    issues = []
    level = AlertLevel.OK

    if error_rate >= thresholds.error_rate_critical:
        level = AlertLevel.CRITICAL
        issues.append(f"error rate {error_rate:.1%} >= critical threshold {thresholds.error_rate_critical:.1%}")
    elif error_rate >= thresholds.error_rate_warning:
        if level != AlertLevel.CRITICAL:
            level = AlertLevel.WARNING
        issues.append(f"error rate {error_rate:.1%} >= warning threshold {thresholds.error_rate_warning:.1%}")

    if latency_seconds >= thresholds.latency_critical_seconds:
        level = AlertLevel.CRITICAL
        issues.append(f"latency {latency_seconds}s >= critical threshold {thresholds.latency_critical_seconds}s")
    elif latency_seconds >= thresholds.latency_warning_seconds:
        if level != AlertLevel.CRITICAL:
            level = AlertLevel.WARNING
        issues.append(f"latency {latency_seconds}s >= warning threshold {thresholds.latency_warning_seconds}s")

    if rows_processed < thresholds.min_rows_processed:
        if level != AlertLevel.CRITICAL:
            level = AlertLevel.WARNING
        issues.append(f"rows processed {rows_processed} < minimum {thresholds.min_rows_processed}")

    message = "; ".join(issues) if issues else "All metrics within acceptable thresholds"

    return PipelineStatus(
        pipeline_name=pipeline.name,
        alert_level=level,
        message=message,
        error_rate=error_rate,
        latency_seconds=latency_seconds,
        rows_processed=rows_processed,
    )
