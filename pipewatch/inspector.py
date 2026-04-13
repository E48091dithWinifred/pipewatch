"""Pipeline inspector: deep-dive field-level diagnostics for a single pipeline status."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class InspectionFinding:
    """A single diagnostic finding for a pipeline field."""

    field_name: str
    value: float
    observation: str
    severity: AlertLevel

    def is_actionable(self) -> bool:
        return self.severity != AlertLevel.OK

    def summary(self) -> str:
        return f"[{self.severity.value.upper()}] {self.field_name}={self.value:.4f} — {self.observation}"


@dataclass
class InspectionReport:
    """Full inspection report for one pipeline."""

    pipeline_name: str
    overall_level: AlertLevel
    findings: List[InspectionFinding] = field(default_factory=list)

    def actionable(self) -> List[InspectionFinding]:
        return [f for f in self.findings if f.is_actionable()]

    def has_issues(self) -> bool:
        return any(f.is_actionable() for f in self.findings)

    def summary(self) -> str:
        lines = [f"Pipeline: {self.pipeline_name} [{self.overall_level.value.upper()}]"]
        for finding in self.findings:
            lines.append(f"  {finding.summary()}")
        return "\n".join(lines)


def _inspect_error_rate(status: PipelineStatus) -> InspectionFinding:
    er = status.error_rate
    thresholds = status.config.thresholds
    if er >= thresholds.error_rate_critical:
        sev = AlertLevel.CRITICAL
        obs = f"Error rate exceeds critical threshold ({thresholds.error_rate_critical})"
    elif er >= thresholds.error_rate_warning:
        sev = AlertLevel.WARNING
        obs = f"Error rate exceeds warning threshold ({thresholds.error_rate_warning})"
    else:
        sev = AlertLevel.OK
        obs = "Error rate within acceptable range"
    return InspectionFinding("error_rate", er, obs, sev)


def _inspect_latency(status: PipelineStatus) -> InspectionFinding:
    lat = status.latency_ms
    thresholds = status.config.thresholds
    if lat >= thresholds.latency_critical_ms:
        sev = AlertLevel.CRITICAL
        obs = f"Latency exceeds critical threshold ({thresholds.latency_critical_ms} ms)"
    elif lat >= thresholds.latency_warning_ms:
        sev = AlertLevel.WARNING
        obs = f"Latency exceeds warning threshold ({thresholds.latency_warning_ms} ms)"
    else:
        sev = AlertLevel.OK
        obs = "Latency within acceptable range"
    return InspectionFinding("latency_ms", lat, obs, sev)


def inspect_pipeline(status: PipelineStatus) -> InspectionReport:
    """Produce a detailed InspectionReport for a single PipelineStatus."""
    findings = [
        _inspect_error_rate(status),
        _inspect_latency(status),
    ]
    return InspectionReport(
        pipeline_name=status.pipeline_name,
        overall_level=status.level,
        findings=findings,
    )


def inspect_all(statuses: List[PipelineStatus]) -> List[InspectionReport]:
    """Inspect every pipeline status and return all reports."""
    return [inspect_pipeline(s) for s in statuses]
