"""summarizer.py — Produce a human-readable text summary of pipeline statuses."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class SummaryLine:
    name: str
    level: str
    error_rate: float
    latency_ms: float
    message: str

    def __str__(self) -> str:
        return (
            f"{self.name:<30} [{self.level:<8}]  "
            f"err={self.error_rate:.2%}  "
            f"lat={self.latency_ms:.1f}ms  "
            f"{self.message}"
        )


def _level_label(level: AlertLevel) -> str:
    return level.value.upper()


def _build_summary_line(status: PipelineStatus) -> SummaryLine:
    return SummaryLine(
        name=status.pipeline_name,
        level=_level_label(status.level),
        error_rate=status.error_rate,
        latency_ms=status.latency_ms,
        message=status.message or "",
    )


def summarize(statuses: List[PipelineStatus]) -> List[SummaryLine]:
    """Convert a list of PipelineStatus objects into SummaryLine records."""
    return [_build_summary_line(s) for s in statuses]


def format_summary(statuses: List[PipelineStatus], *, title: str = "Pipeline Summary") -> str:
    """Return a formatted multi-line string summary of all pipeline statuses."""
    lines = summarize(statuses)
    ok = sum(1 for s in statuses if s.level == AlertLevel.OK)
    warning = sum(1 for s in statuses if s.level == AlertLevel.WARNING)
    critical = sum(1 for s in statuses if s.level == AlertLevel.CRITICAL)

    header = (
        f"{'=' * 60}\n"
        f"  {title}\n"
        f"  Total: {len(statuses)}  OK: {ok}  Warning: {warning}  Critical: {critical}\n"
        f"{'=' * 60}"
    )
    body = "\n".join(str(line) for line in lines) if lines else "  (no pipelines)"
    return f"{header}\n{body}\n{'=' * 60}"


def print_summary(statuses: List[PipelineStatus], *, title: str = "Pipeline Summary") -> None:
    """Print the formatted summary to stdout."""
    print(format_summary(statuses, title=title))


def filter_by_level(statuses: List[PipelineStatus], level: AlertLevel) -> List[PipelineStatus]:
    """Return only the pipeline statuses matching the given alert level.

    Args:
        statuses: The full list of pipeline statuses to filter.
        level: The alert level to filter by (e.g. AlertLevel.CRITICAL).

    Returns:
        A list containing only statuses whose level matches the given level.
    """
    return [s for s in statuses if s.level == level]
