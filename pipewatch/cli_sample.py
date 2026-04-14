"""pipewatch/cli_sample.py — CLI sub-command: pipewatch sample."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.sampler import SamplerConfig, sample_statuses


def _build_sample_parser(sub: argparse._SubParsersAction) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    p = sub.add_parser("sample", help="Randomly sample pipeline statuses")
    p.add_argument("input", help="JSON file of pipeline statuses")
    p.add_argument("--rate", type=float, default=1.0, help="Fraction to keep (0.0–1.0)")
    p.add_argument("--seed", type=int, default=None, help="Random seed for deterministic sampling")
    p.add_argument("--min-keep", type=int, default=0, dest="min_keep", help="Minimum statuses to keep")
    p.add_argument("--output", default=None, help="Write kept statuses to JSON file")
    p.add_argument("--summary", action="store_true", help="Print summary line only")
    return p


def _load_statuses(path: str) -> List[PipelineStatus]:
    p = Path(path)
    if not p.exists():
        print(f"[error] file not found: {path}", file=sys.stderr)
        return []
    raw = json.loads(p.read_text())
    statuses = []
    for item in raw:
        statuses.append(
            PipelineStatus(
                pipeline_name=item["pipeline_name"],
                level=AlertLevel[item["level"]],
                error_rate=item.get("error_rate", 0.0),
                latency_ms=item.get("latency_ms", 0.0),
                message=item.get("message", ""),
            )
        )
    return statuses


def cmd_sample(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.input)
    if not statuses:
        print("No statuses to sample.")
        return

    try:
        cfg = SamplerConfig(rate=args.rate, seed=args.seed, min_keep=args.min_keep)
    except ValueError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)

    result = sample_statuses(statuses, cfg)

    if args.summary:
        print(result.summary)
        return

    if args.output:
        out = [
            {
                "pipeline_name": s.pipeline_name,
                "level": s.level.name,
                "error_rate": s.error_rate,
                "latency_ms": s.latency_ms,
                "message": s.message,
            }
            for s in result.kept
        ]
        Path(args.output).write_text(json.dumps(out, indent=2))
        print(f"Wrote {result.kept_count} statuses to {args.output}")
    else:
        for s in result.kept:
            print(f"  [{s.level.name}] {s.pipeline_name}")
        print(result.summary)
