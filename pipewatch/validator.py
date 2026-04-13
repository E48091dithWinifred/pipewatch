"""Pipeline status validator — checks statuses against defined rules and reports violations."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class ValidationRule:
    name: str
    max_error_rate: Optional[float] = None
    max_latency_ms: Optional[float] = None
    forbidden_levels: List[str] = field(default_factory=list)


@dataclass
class ValidationViolation:
    pipeline: str
    rule: str
    message: str

    @property
    def summary(self) -> str:
        return f"[{self.pipeline}] {self.rule}: {self.message}"


@dataclass
class ValidationReport:
    pipeline: str
    violations: List[ValidationViolation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0


def validate_status(
    status: PipelineStatus, rules: List[ValidationRule]
) -> ValidationReport:
    """Run all rules against a single pipeline status."""
    report = ValidationReport(pipeline=status.pipeline_name)

    for rule in rules:
        if (
            rule.max_error_rate is not None
            and status.error_rate is not None
            and status.error_rate > rule.max_error_rate
        ):
            report.violations.append(
                ValidationViolation(
                    pipeline=status.pipeline_name,
                    rule=rule.name,
                    message=(
                        f"error_rate {status.error_rate:.4f} exceeds "
                        f"max {rule.max_error_rate:.4f}"
                    ),
                )
            )

        if (
            rule.max_latency_ms is not None
            and status.latency_ms is not None
            and status.latency_ms > rule.max_latency_ms
        ):
            report.violations.append(
                ValidationViolation(
                    pipeline=status.pipeline_name,
                    rule=rule.name,
                    message=(
                        f"latency_ms {status.latency_ms:.1f} exceeds "
                        f"max {rule.max_latency_ms:.1f}"
                    ),
                )
            )

        if status.level.value in rule.forbidden_levels:
            report.violations.append(
                ValidationViolation(
                    pipeline=status.pipeline_name,
                    rule=rule.name,
                    message=f"level '{status.level.value}' is forbidden by rule",
                )
            )

    return report


def validate_all(
    statuses: List[PipelineStatus], rules: List[ValidationRule]
) -> List[ValidationReport]:
    """Validate every status and return all reports (including passing ones)."""
    return [validate_status(s, rules) for s in statuses]
