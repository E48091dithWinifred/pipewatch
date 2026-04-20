"""pipewatch.cli_clamp — CLI subcommand: clamp pipeline metric values."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.clamper import ClampConfig, clamp_statuses


def _build_clamp_parser(sub: "argparse._SubParsersAction") -> argparse.ArgumentParser:  # type: ignore[type-arg]
    p = sub.add_parser("clamp", help="Clamp pipeline metric values to configured bounds")
    p.add_argument("input", help="JSON file of pipeline statuses")
    p.add_argument("--min-error-rate", type=float, default=0.0, metavar="F")
    p.add_argument("--max-error-rate", type=float, default=1.0, metavar="F")
    p.add_argument("--min-latency-ms", type=float, default=0.0, metavar="F")
    p.add_argument("--max-latency-ms", type=float, default=60_000.0, metavar="F")
    p.add_argument("--summary", action="store_true", help="Print summary line only")
    return p


def _load_statuses(path: str) -> List[PipelineStatus]:
    p = Path(path)
    if not p.exists():
        print(f"[error] file not found: {path}", file=sys.stderr)
        sys.exit(1)
    raw = json.loads(p.read_text())
    return [
        PipelineStatus(
            name=r["name"],
            level=AlertLevel[r["level"].upper()],
            message=r.get("message", ""),
            error_rate=r.get("error_rate", 0.0),
            latency_ms=r.get("latency_ms", 0.0),
        )
        for r in raw
    ]


def cmd_clamp(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.input)
    try:
        cfg = ClampConfig(
            min_error_rate=args.min_error_rate,
            max_error_rate=args.max_error_rate,
            min_latency_ms=args.min_latency_ms,
            max_latency_ms=args.max_latency_ms,
        )
    except ValueError as exc:
        print(f"[error] invalid clamp config: {exc}", file=sys.stderr)
        sys.exit(1)

    result = clamp_statuses(statuses, cfg)

    if args.summary:
        print(result.summary())
        return

    for cs in result.statuses:
        tag = " [CLAMPED]" if cs.was_clamped() else ""
        print(
            f"{cs.name:<30} error_rate={cs.error_rate:.4f}  "
            f"latency_ms={cs.latency_ms:.1f}{tag}"
        )
    print()
    print(result.summary())
