"""Entry-point CLI for pipewatch — run checks and record history."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipewatch.checker import AlertLevel, check_pipeline
from pipewatch.config import load_config
from pipewatch.history import RunRecord, append_run, clear_history, load_history
from pipewatch.metrics import PipelineMetrics
from pipewatch.reporter import print_report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Monitor ETL pipeline health with configurable alerting.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Run health checks defined in config.")
    run_p.add_argument("--config", default="pipewatch.yaml", help="Path to config file.")
    run_p.add_argument("--no-color", action="store_true", help="Disable colored output.")
    run_p.add_argument("--no-history", action="store_true", help="Skip writing to history.")

    hist_p = sub.add_parser("history", help="Show stored run history.")
    hist_p.add_argument("--pipeline", default=None, help="Filter by pipeline name.")
    hist_p.add_argument("--limit", type=int, default=20, help="Max records to display.")
    hist_p.add_argument("--clear", action="store_true", help="Delete all history records.")

    return parser


def cmd_run(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    statuses = []

    for pipeline_cfg in config.pipelines:
        # In a real tool metrics would be fetched from a live source;
        # here we construct a placeholder from config defaults so the
        # CLI wiring is exercisable end-to-end.
        metrics = PipelineMetrics(
            pipeline_name=pipeline_cfg.name,
            total_records=1000,
            failed_records=0,
            start_time=0.0,
            end_time=1.0,
        )
        status = check_pipeline(metrics, pipeline_cfg.thresholds)
        statuses.append(status)

        if not args.no_history:
            record = RunRecord(
                pipeline_name=status.pipeline_name,
                timestamp=RunRecord.now_iso(),
                alert_level=status.level.name,
                error_rate=status.metrics.error_rate,
                latency_ms=status.metrics.latency_ms,
                rows_per_second=status.metrics.rows_per_second,
                message=status.message,
            )
            append_run(record)

    print_report(statuses, use_color=not args.no_color)
    has_critical = any(s.level == AlertLevel.CRITICAL for s in statuses)
    return 1 if has_critical else 0


def cmd_history(args: argparse.Namespace) -> int:
    if args.clear:
        clear_history()
        print("History cleared.")
        return 0

    records = load_history(pipeline_name=args.pipeline, limit=args.limit)
    if not records:
        print("No history records found.")
        return 0

    for rec in records:
        tag = f"[{rec.alert_level}]".ljust(10)
        print(f"{rec.timestamp}  {tag}  {rec.pipeline_name}  err={rec.error_rate:.3f}  lat={rec.latency_ms:.1f}ms")
    return 0


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "run":
        sys.exit(cmd_run(args))
    elif args.command == "history":
        sys.exit(cmd_history(args))


if __name__ == "__main__":  # pragma: no cover
    main()
