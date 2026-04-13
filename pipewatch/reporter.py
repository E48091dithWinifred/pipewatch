"""Formats and prints pipeline health reports to stdout."""

from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus

_LEVEL_COLORS = {
    AlertLevel.OK: "\033[92m",       # green
    AlertLevel.WARNING: "\033[93m",  # yellow
    AlertLevel.CRITICAL: "\033[91m", # red
}
_RESET = "\033[0m"


def _colorize(text: str, level: AlertLevel, use_color: bool = True) -> str:
    if not use_color:
        return text
    color = _LEVEL_COLORS.get(level, "")
    return f"{color}{text}{_RESET}"


def format_status_line(status: PipelineStatus, use_color: bool = True) -> str:
    """Return a single formatted line summarising *status*."""
    level_tag = _colorize(f"[{status.level.name}]", status.level, use_color)
    messages = "; ".join(status.messages) if status.messages else "all checks passed"
    return f"{level_tag} {status.pipeline_name}: {messages}"


def print_report(
    statuses: List[PipelineStatus],
    use_color: bool = True,
    verbose: bool = False,
) -> int:
    """Print a health report for all pipelines.

    Returns the number of pipelines that are not OK.
    """
    issues = 0
    for status in statuses:
        line = format_status_line(status, use_color=use_color)
        print(line)
        if verbose and status.metrics:
            m = status.metrics
            print(
                f"    error_rate={m.error_rate:.2%}  "
                f"latency={m.latency_ms:.1f}ms  "
                f"processed={m.records_processed}"
            )
        if status.level != AlertLevel.OK:
            issues += 1
    return issues
