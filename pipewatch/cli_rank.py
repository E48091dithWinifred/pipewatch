"""CLI sub-command: rank — display pipelines ordered by health score."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.ranker import RankedPipeline, rank_pipelines

_LEVEL_COLOR = {
    AlertLevel.OK: "\033[32m",
    AlertLevel.WARNING: "\033[33m",
    AlertLevel.CRITICAL: "\033[31m",
}
_RESET = "\033[0m"


def _build_rank_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("rank", help="Rank pipelines from worst to best health")
    p.add_argument("input", type=Path, help="JSON file with pipeline statuses")
    p.add_argument(
        "--top",
        type=int,
        default=None,
        metavar="N",
        help="Show only the N worst pipelines",
    )
    p.add_argument(
        "--no-color",
        action="store_true",
        default=False,
        help="Disable ANSI color output",
    )


def _load_statuses(path: Path) -> List[PipelineStatus]:
    raw = json.loads(path.read_text())
    statuses = []
    for item in raw:
        level = AlertLevel[item["level"].upper()]
        from pipewatch.metrics import PipelineMetrics

        metrics = None
        if item.get("metrics"):
            m = item["metrics"]
            metrics = PipelineMetrics(
                pipeline_name=item["pipeline_name"],
                total_records=m.get("total_records", 0),
                failed_records=m.get("failed_records", 0),
                duration_seconds=m.get("duration_seconds", 1.0),
                latency_ms=m.get("latency_ms", 0.0),
            )
        statuses.append(
            PipelineStatus(
                pipeline_name=item["pipeline_name"],
                level=level,
                message=item.get("message", ""),
                metrics=metrics,
            )
        )
    return statuses


def _format_row(r: RankedPipeline, color: bool) -> str:
    prefix = _LEVEL_COLOR.get(r.level, "") if color else ""
    suffix = _RESET if color else ""
    return (
        f"{r.rank:>3}. {prefix}{r.name:<30}{suffix} "
        f"grade={r.grade} score={r.score:5.1f} "
        f"err={r.error_rate:.2%} lat={r.latency_ms:.0f}ms "
        f"[{r.level.name}]"
    )


def cmd_rank(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.input)
    ranked = rank_pipelines(statuses, top_n=args.top)

    if not ranked:
        print("No pipelines to rank.")
        return

    label = f"Top {args.top}" if args.top else "All"
    print(f"Pipeline Rankings ({label} pipelines, worst first)")
    print("-" * 60)
    for r in ranked:
        print(_format_row(r, color=not args.no_color))
