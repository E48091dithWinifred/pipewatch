"""CLI sub-commands: baseline capture and drift check."""
from __future__ import annotations

import argparse
import json
import sys
from typing import List

from pipewatch.baseline import capture_baseline, load_baseline, compare_to_baseline
from pipewatch.checker import AlertLevel, PipelineStatus


def _build_baseline_parser(sub: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p_cap = sub.add_parser("baseline-capture", help="Capture current metrics as a baseline")
    p_cap.add_argument("statuses_file", help="JSON file of pipeline statuses")
    p_cap.add_argument("--dir", default=".pipewatch", help="Directory to store baseline files")
    p_cap.add_argument("--tag", default="default", help="Baseline tag/name")
    p_cap.set_defaults(func=cmd_baseline_capture)

    p_diff = sub.add_parser("baseline-drift", help="Compare current metrics to a stored baseline")
    p_diff.add_argument("statuses_file", help="JSON file of pipeline statuses")
    p_diff.add_argument("--dir", default=".pipewatch", help="Directory containing baseline files")
    p_diff.add_argument("--tag", default="default", help="Baseline tag/name")
    p_diff.add_argument("--no-color", action="store_true")
    p_diff.set_defaults(func=cmd_baseline_drift)


def _load_statuses(path: str) -> List[PipelineStatus]:
    with open(path) as fh:
        raw = json.load(fh)
    return [
        PipelineStatus(
            pipeline_name=r["pipeline_name"],
            level=AlertLevel[r["level"]],
            error_rate=r["error_rate"],
            latency_ms=r["latency_ms"],
            message=r.get("message", ""),
        )
        for r in raw
    ]


def cmd_baseline_capture(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.statuses_file)
    entries = capture_baseline(statuses, args.dir, tag=args.tag)
    print(f"Baseline '{args.tag}' captured for {len(entries)} pipeline(s) in '{args.dir}'.")


def cmd_baseline_drift(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.statuses_file)
    baseline = load_baseline(args.dir, tag=args.tag)
    if baseline is None:
        print(f"No baseline found for tag '{args.tag}' in '{args.dir}'.", file=sys.stderr)
        sys.exit(1)

    report = compare_to_baseline(statuses, baseline)
    if not report.drifts:
        print("No pipelines matched baseline entries.")
        return

    for drift in report.drifts:
        er_sign = "+" if drift.error_rate_delta >= 0 else ""
        lat_sign = "+" if drift.latency_delta_ms >= 0 else ""
        flag = "[DRIFT]" if drift.has_drift else "[OK]   "
        print(
            f"{flag} {drift.pipeline_name:<30} "
            f"err_rate {er_sign}{drift.error_rate_delta:.4f}  "
            f"latency {lat_sign}{drift.latency_delta_ms:.1f}ms"
        )

    degraded = report.degraded
    if degraded:
        print(f"\n{len(degraded)} pipeline(s) show degradation vs baseline '{args.tag}'.")
        sys.exit(2)
    else:
        print("\nAll pipelines within baseline tolerance.")
