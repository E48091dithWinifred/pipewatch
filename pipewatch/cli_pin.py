"""CLI commands for pinning/unpinning pipelines."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipewatch.pinner import PinConfig, filter_pinned, pin_pipeline, unpin_pipeline
from pipewatch.checker import AlertLevel, PipelineStatus


def _build_pin_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipewatch pin", description="Pin/unpin pipelines")
    sub = p.add_subparsers(dest="subcmd", required=True)

    add = sub.add_parser("add", help="Pin a pipeline")
    add.add_argument("pipeline", help="Pipeline name")
    add.add_argument("--expires-at", default=None, help="ISO expiry timestamp")
    add.add_argument("--reason", default="", help="Reason for pinning")
    add.add_argument("--state", default=".pipewatch/pin_state.json")

    rm = sub.add_parser("remove", help="Unpin a pipeline")
    rm.add_argument("pipeline", help="Pipeline name")
    rm.add_argument("--state", default=".pipewatch/pin_state.json")

    ls = sub.add_parser("list", help="List pinned pipelines from a status file")
    ls.add_argument("status_file", help="JSON file with pipeline statuses")
    ls.add_argument("--state", default=".pipewatch/pin_state.json")

    return p


def _load_statuses(path: str) -> list[PipelineStatus]:
    p = Path(path)
    if not p.exists():
        return []
    raw = json.loads(p.read_text())
    return [
        PipelineStatus(
            pipeline_name=r["pipeline_name"],
            level=AlertLevel[r["level"]],
            message=r.get("message", ""),
            error_rate=r.get("error_rate", 0.0),
            latency_ms=r.get("latency_ms", 0.0),
        )
        for r in raw
    ]


def cmd_pin(argv: list[str] | None = None) -> None:
    parser = _build_pin_parser()
    args = parser.parse_args(argv)
    cfg = PinConfig(state_path=args.state)

    if args.subcmd == "add":
        entry = pin_pipeline(
            args.pipeline, cfg, expires_at=args.expires_at, reason=args.reason
        )
        print(f"Pinned '{entry.pipeline}' at {entry.pinned_at}")
        if entry.expires_at:
            print(f"  Expires: {entry.expires_at}")
        if entry.reason:
            print(f"  Reason: {entry.reason}")

    elif args.subcmd == "remove":
        removed = unpin_pipeline(args.pipeline, cfg)
        if removed:
            print(f"Unpinned '{args.pipeline}'")
        else:
            print(f"'{args.pipeline}' was not pinned", file=sys.stderr)
            sys.exit(1)

    elif args.subcmd == "list":
        statuses = _load_statuses(args.status_file)
        unpinned, pinned = filter_pinned(statuses, cfg)
        print(f"Pinned ({len(pinned)}):")
        for s in pinned:
            print(f"  {s.pipeline_name}  [{s.level.name}]")
        print(f"Active ({len(unpinned)}):")
        for s in unpinned:
            print(f"  {s.pipeline_name}  [{s.level.name}]")


if __name__ == "__main__":
    cmd_pin()
