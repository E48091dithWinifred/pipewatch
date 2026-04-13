"""CLI sub-command: ``pipewatch score`` — display health scores for pipelines."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.scorer import PipelineScore, score_all

_GRADE_COLOR = {"A": "\033[92m", "B": "\033[96m", "C": "\033[93m", "D": "\033[33m", "F": "\033[91m"}
_RESET = "\033[0m"


def _build_score_parser(sub: "argparse._SubParsersAction") -> argparse.ArgumentParser:  # type: ignore[type-arg]
    p = sub.add_parser("score", help="Show numeric health scores for pipelines")
    p.add_argument("input", help="JSON file produced by 'pipewatch run --json'")
    p.add_argument("--no-color", action="store_true", help="Disable ANSI colour output")
    p.add_argument("--min-score", type=float, default=0.0, help="Only show pipelines below this score")
    p.add_argument("--latency-ceiling", type=float, default=5000.0, metavar="MS",
                   help="Latency value (ms) that maps to a 0 latency sub-score (default: 5000)")
    return p


def _load_statuses(path: str) -> List[PipelineStatus]:
    data = json.loads(Path(path).read_text())
    statuses: List[PipelineStatus] = []
    for item in data:
        statuses.append(
            PipelineStatus(
                pipeline=item["pipeline"],
                level=AlertLevel[item["level"]],
                error_rate=float(item.get("error_rate", 0.0)),
                latency_ms=float(item.get("latency_ms", 0.0)),
                message=item.get("message", ""),
            )
        )
    return statuses


def _format_row(ps: PipelineScore, color: bool) -> str:
    grade_str = f"[{ps.grade}]"
    if color:
        c = _GRADE_COLOR.get(ps.grade, "")
        grade_str = f"{c}{grade_str}{_RESET}"
    return (
        f"{grade_str:<12} {ps.score:>6.1f}  "
        f"err={ps.error_rate*100:.1f}%  "
        f"lat={ps.latency_ms:.0f}ms  "
        f"{ps.pipeline}"
    )


def cmd_score(args: argparse.Namespace) -> None:
    try:
        statuses = _load_statuses(args.input)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)

    scores = score_all(statuses, latency_ceiling_ms=args.latency_ceiling)
    filtered = [ps for ps in scores if ps.score <= args.min_score] if args.min_score > 0 else scores

    if not filtered:
        print("No pipelines match the given criteria.")
        return

    header = f"{'Grade':<12} {'Score':>6}  {'Error Rate':<12} {'Latency':<12} Pipeline"
    print(header)
    print("-" * len(header))
    for ps in filtered:
        print(_format_row(ps, color=not args.no_color))
