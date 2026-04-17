"""cli_tail.py — CLI command to tail recent pipeline history records."""
from __future__ import annotations
import argparse
import json
from pipewatch.tailer import TailConfig, tail_history


def _build_tail_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipewatch tail",
                                description="Tail recent pipeline history")
    p.add_argument("history", help="Path to history JSON file")
    p.add_argument("-n", "--count", type=int, default=10,
                   help="Number of records to show (default: 10)")
    p.add_argument("--pipeline", default=None,
                   help="Filter to a specific pipeline name")
    p.add_argument("--json", dest="as_json", action="store_true",
                   help="Output records as JSON")
    return p


def cmd_tail(args: argparse.Namespace) -> None:
    config = TailConfig(n=args.count, pipeline=args.pipeline)
    result = tail_history(args.history, config)
    print(result.summary())
    if args.as_json:
        rows = [
            {"pipeline": r.pipeline, "level": r.level,
             "timestamp": r.timestamp, "error_rate": r.error_rate,
             "latency_ms": r.latency_ms, "rows_processed": r.rows_processed}
            for r in result.records
        ]
        print(json.dumps(rows, indent=2))
    else:
        for r in result.records:
            print(f"  [{r.timestamp}] {r.pipeline:<30} {r.level.upper():<10} "
                  f"err={r.error_rate:.3f} lat={r.latency_ms:.1f}ms")


def main() -> None:
    parser = _build_tail_parser()
    args = parser.parse_args()
    cmd_tail(args)


if __name__ == "__main__":
    main()
