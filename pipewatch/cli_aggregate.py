"""CLI sub-command: aggregate — print summary stats across all pipelines."""
from __future__ import annotations

import argparse
import json
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.metrics import PipelineMetrics
from pipewatch.aggregator import aggregate, group_by_level


def _build_aggregate_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "aggregate",
        help="Print aggregated statistics across pipeline statuses.",
    )
    p.add_argument(
        "--input",
        metavar="FILE",
        default=None,
        help="JSON file produced by 'export' command (default: stdin).",
    )
    p.add_argument(
        "--group",
        action="store_true",
        help="Also print pipelines grouped by alert level.",
    )
    p.set_defaults(func=cmd_aggregate)


def _load_statuses(path: str | None) -> List[PipelineStatus]:
    import sys

    if path:
        with open(path) as fh:
            raw = json.load(fh)
    else:
        raw = json.load(sys.stdin)

    statuses: List[PipelineStatus] = []
    for item in raw:
        metrics = PipelineMetrics(
            pipeline_name=item["pipeline_name"],
            total_records=item.get("total_records", 1000),
            failed_records=int(item.get("error_rate", 0.0) * item.get("total_records", 1000)),
            duration_seconds=item.get("duration_seconds", 1.0),
            latency_p99_ms=item.get("latency_ms", 0.0),
        )
        statuses.append(
            PipelineStatus(
                pipeline_name=item["pipeline_name"],
                level=AlertLevel(item["level"]),
                message=item.get("message", ""),
                metrics=metrics,
            )
        )
    return statuses


def cmd_aggregate(args: argparse.Namespace) -> None:
    statuses = _load_statuses(getattr(args, "input", None))
    stats = aggregate(statuses)

    print(f"Total pipelines : {stats.total}")
    print(f"OK              : {stats.ok_count}")
    print(f"Warning         : {stats.warning_count}")
    print(f"Critical        : {stats.critical_count}")
    print(f"Health ratio    : {stats.health_ratio:.1%}")
    print(f"Avg error rate  : {stats.avg_error_rate:.4f}")
    print(f"Max error rate  : {stats.max_error_rate:.4f}")
    print(f"Avg latency ms  : {stats.avg_latency_ms:.1f}")
    print(f"Max latency ms  : {stats.max_latency_ms:.1f}")

    if getattr(args, "group", False):
        groups = group_by_level(statuses)
        for level, pipelines in groups.items():
            names = ", ".join(p.pipeline_name for p in pipelines) or "—"
            print(f"  [{level.upper():8s}] {names}")
