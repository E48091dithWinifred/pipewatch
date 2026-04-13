"""CLI sub-command: dedupe — filter a status JSON file for repeated alerts."""

from __future__ import annotations

import argparse
import json
import sys
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.deduplicator import DedupeConfig, filter_statuses


def _build_dedupe_parser(sub: "argparse._SubParsersAction") -> argparse.ArgumentParser:  # type: ignore[type-arg]
    p = sub.add_parser("dedupe", help="Suppress repeated non-critical alerts")
    p.add_argument("statuses", help="Path to JSON file with pipeline statuses")
    p.add_argument(
        "--state",
        default=".pipewatch_dedupe.json",
        help="Path to dedupe state file (default: .pipewatch_dedupe.json)",
    )
    p.add_argument(
        "--min-repeat",
        type=int,
        default=3,
        dest="min_repeat",
        help="Number of consecutive hits before alert fires (default: 3)",
    )
    p.add_argument(
        "--output",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    return p


def _load_statuses(path: str) -> List[PipelineStatus]:
    try:
        with open(path) as fh:
            data = json.load(fh)
    except FileNotFoundError:
        print(f"[error] File not found: {path}", file=sys.stderr)
        sys.exit(1)
    return [
        PipelineStatus(
            pipeline=d["pipeline"],
            level=AlertLevel(d["level"]),
            message=d.get("message", ""),
        )
        for d in data
    ]


def cmd_dedupe(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.statuses)
    cfg = DedupeConfig(state_path=args.state, min_repeat=args.min_repeat)
    passed = filter_statuses(statuses, cfg)

    if not passed:
        print("No alerts passed deduplication filter.")
        return

    if args.output == "json":
        out = [
            {"pipeline": s.pipeline, "level": s.level.value, "message": s.message}
            for s in passed
        ]
        print(json.dumps(out, indent=2))
    else:
        for s in passed:
            print(f"[{s.level.value.upper()}] {s.pipeline}: {s.message}")
