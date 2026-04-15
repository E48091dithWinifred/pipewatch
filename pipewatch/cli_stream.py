"""CLI command for streaming pipeline statuses with filtering options."""
from __future__ import annotations

import argparse
import json
import sys
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.streamer import StreamConfig, batch_stream, stream_statuses


def _build_stream_parser(sub: "argparse._SubParsersAction") -> argparse.ArgumentParser:  # type: ignore[type-arg]
    p = sub.add_parser("stream", help="Stream pipeline statuses with filtering")
    p.add_argument("input", help="Path to JSON status file")
    p.add_argument("--drop-ok", action="store_true", help="Drop OK-level statuses")
    p.add_argument("--max-items", type=int, default=None, help="Max statuses to emit")
    p.add_argument("--batch-size", type=int, default=0, help="Print in batches (0=no batching)")
    p.add_argument("--summary", action="store_true", help="Print stream summary only")
    return p


def _load_statuses(path: str) -> List[PipelineStatus]:
    try:
        with open(path) as fh:
            records = json.load(fh)
    except FileNotFoundError:
        print(f"error: file not found: {path}", file=sys.stderr)
        return []
    statuses = []
    for r in records:
        statuses.append(
            PipelineStatus(
                pipeline_name=r["pipeline_name"],
                level=AlertLevel[r["level"].upper()],
                error_rate=r.get("error_rate", 0.0),
                latency_ms=r.get("latency_ms", 0.0),
                message=r.get("message", ""),
            )
        )
    return statuses


def cmd_stream(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.input)
    cfg = StreamConfig(
        drop_ok=args.drop_ok,
        max_items=args.max_items,
        batch_size=args.batch_size if args.batch_size > 0 else len(statuses) or 1,
    )

    result = stream_statuses(statuses, cfg=cfg)

    if args.summary:
        print(result.summary())
        return

    if args.batch_size > 0:
        for i, batch in enumerate(batch_stream(result.emitted, batch_size=args.batch_size), 1):
            print(f"--- batch {i} ---")
            for s in batch:
                print(f"  [{s.level.value.upper()}] {s.pipeline_name}: {s.message}")
    else:
        for s in result.emitted:
            print(f"[{s.level.value.upper()}] {s.pipeline_name}: {s.message}")

    print(f"\n{result.summary()}")
