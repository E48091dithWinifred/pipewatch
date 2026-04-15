"""CLI command: pipewatch reap — remove stale pipeline statuses."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.reaper import ReaperConfig, reap_statuses


def _build_reap_parser(sub: argparse._SubParsersAction) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    p = sub.add_parser("reap", help="Remove stale pipeline statuses by age")
    p.add_argument("input", help="JSON file with pipeline statuses")
    p.add_argument(
        "--max-age",
        type=float,
        default=3600.0,
        metavar="SECONDS",
        help="Maximum age in seconds before a status is reaped (default: 3600)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be reaped without modifying output",
    )
    p.add_argument(
        "--output",
        default=None,
        metavar="FILE",
        help="Write kept statuses to FILE (default: stdout)",
    )
    return p


def _load_statuses(path: str) -> List[PipelineStatus]:
    p = Path(path)
    if not p.exists():
        return []
    raw = json.loads(p.read_text())
    statuses = []
    for item in raw:
        statuses.append(
            PipelineStatus(
                pipeline_name=item["pipeline_name"],
                level=AlertLevel[item["level"]],
                message=item.get("message", ""),
                error_rate=item.get("error_rate", 0.0),
                latency_ms=item.get("latency_ms", 0.0),
                checked_at=item.get("checked_at"),
            )
        )
    return statuses


def cmd_reap(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.input)
    cfg = ReaperConfig(max_age_seconds=args.max_age, dry_run=args.dry_run)
    result = reap_statuses(statuses, config=cfg)

    print(f"[reap] {result.summary()}", file=sys.stderr)
    for s in result.reaped:
        print(f"  reaped: {s.pipeline_name} (checked_at={s.checked_at})", file=sys.stderr)

    if args.dry_run:
        return

    output_data = [
        {
            "pipeline_name": s.pipeline_name,
            "level": s.level.name,
            "message": s.message,
            "error_rate": s.error_rate,
            "latency_ms": s.latency_ms,
            "checked_at": s.checked_at,
        }
        for s in result.kept
    ]
    payload = json.dumps(output_data, indent=2)
    if args.output:
        Path(args.output).write_text(payload)
    else:
        print(payload)
