"""CLI sub-command: pipewatch compare — diff two exported status JSON files."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.comparator import compare_runs


def _build_compare_parser(sub: argparse._SubParsersAction) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    p = sub.add_parser("compare", help="Compare two status export JSON files")
    p.add_argument("previous", help="Path to previous run JSON export")
    p.add_argument("current", help="Path to current run JSON export")
    p.add_argument(
        "--regressions-only",
        action="store_true",
        help="Exit with code 1 if regressions detected",
    )
    return p


def _load_statuses(path: str) -> List[PipelineStatus]:
    data = json.loads(Path(path).read_text())
    statuses: List[PipelineStatus] = []
    for item in data:
        statuses.append(
            PipelineStatus(
                pipeline_name=item["pipeline_name"],
                level=AlertLevel[item["level"].upper()],
                error_rate=float(item.get("error_rate", 0.0)),
                latency_ms=float(item.get("latency_ms", 0.0)),
                message=item.get("message", ""),
            )
        )
    return statuses


def cmd_compare(args: argparse.Namespace) -> None:
    previous = _load_statuses(args.previous)
    current = _load_statuses(args.current)
    report = compare_runs(previous, current)

    if report.added:
        print(f"Added pipelines   : {', '.join(report.added)}")
    if report.removed:
        print(f"Removed pipelines : {', '.join(report.removed)}")

    if not report.changes:
        print("No status changes detected.")
    else:
        for c in report.changes:
            arrow = "↑" if c.degraded else "↓" if c.improved else "~"
            print(
                f"  {arrow} {c.name}: {c.previous_level.name} -> {c.current_level.name}"
                f"  (error_rate delta: {c.error_rate_delta:+.4f})"
            )

    if args.regressions_only and report.has_regressions:
        sys.exit(1)
