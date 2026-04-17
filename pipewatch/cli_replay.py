"""CLI command for replaying pipeline run history."""
from __future__ import annotations
import argparse
import json
from pipewatch.replayer import ReplayConfig, replay
from pipewatch.history import RunRecord


def _build_replay_parser(sub: argparse.ArgumentParser) -> None:
    sub.add_argument("history_file", help="Path to history JSON file")
    sub.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    sub.add_argument("--max-runs", type=int, default=50, help="Max runs to replay")
    sub.add_argument("--reverse", action="store_true", help="Replay in reverse order")
    sub.add_argument("--json", dest="as_json", action="store_true", help="Output as JSON")


def _print_record(record: RunRecord) -> None:
    print(f"  [{record.checked_at}] {record.pipeline_name} -> {record.level} (err={record.error_rate:.3f}, lat={record.latency_ms:.1f}ms)")


def cmd_replay(args: argparse.Namespace) -> None:
    cfg = ReplayConfig(
        history_file=args.history_file,
        pipeline_name=args.pipeline,
        max_runs=args.max_runs,
        reverse=args.reverse,
    )
    result = replay(cfg)
    if args.as_json:
        rows = [
            {
                "pipeline_name": r.pipeline_name,
                "checked_at": r.checked_at,
                "level": r.level,
                "error_rate": r.error_rate,
                "latency_ms": r.latency_ms,
            }
            for r in result.records
        ]
        print(json.dumps(rows, indent=2))
    else:
        print(result.summary())
        for record in result.records:
            _print_record(record)
