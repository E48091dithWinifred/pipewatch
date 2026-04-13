"""Formatting helpers for CLI output."""

from __future__ import annotations

from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.digest import Digest, PipelineDigest

_RESET = "\033[0m"
_COLORS = {
    AlertLevel.OK: "\033[32m",       # green
    AlertLevel.WARNING: "\033[33m",  # yellow
    AlertLevel.CRITICAL: "\033[31m", # red
}


def _colorize(text: str, level: AlertLevel) -> str:
    color = _COLORS.get(level, "")
    return f"{color}{text}{_RESET}"


def format_status_line(status: PipelineStatus, use_color: bool = False) -> str:
    label = status.level.value.upper().ljust(8)
    msg = f" — {status.message}" if status.message else ""
    line = f"[{label}] {status.pipeline_name}{msg}"
    if use_color:
        line = _colorize(line, status.level)
    return line


def print_report(statuses: List[PipelineStatus], use_color: bool = False) -> None:
    for s in statuses:
        print(format_status_line(s, use_color=use_color))


def format_digest_line(pd: PipelineDigest, use_color: bool = False) -> str:
    trend_symbol = ""
    if pd.trend:
        if pd.trend.is_degrading:
            trend_symbol = " ↑err"
        elif pd.trend.is_improving:
            trend_symbol = " ↓err"

    line = (
        f"{pd.pipeline_name:<20} "
        f"runs={pd.total_runs:<4} "
        f"ok={pd.ok_count:<4} "
        f"warn={pd.warning_count:<4} "
        f"crit={pd.critical_count:<4} "
        f"score={pd.health_score:<6} "
        f"{pd.sparkline}{trend_symbol}"
    )
    return line


def print_digest(digest: Digest, use_color: bool = False) -> None:
    header = f"=== Digest (last {digest.window_hours}h) ==="
    print(header)
    for pd in digest.pipelines:
        print(format_digest_line(pd, use_color=use_color))
    if digest.most_critical:
        mc = digest.most_critical
        suffix = f" (score {mc.health_score})"
        print(f"\nLowest health: {mc.pipeline_name}{suffix}")
