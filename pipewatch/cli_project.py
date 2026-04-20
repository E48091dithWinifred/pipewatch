"""cli_project.py — CLI command for projecting future pipeline health."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.history import RunRecord
from pipewatch.projector import project_all, ProjectionResult


def _build_project_parser(sub: "argparse._SubParsersAction") -> argparse.ArgumentParser:  # type: ignore[type-arg]
    p = sub.add_parser("project", help="Project future pipeline health from history")
    p.add_argument("history_file", help="Path to history JSON file")
    p.add_argument("--steps", type=int, default=3, help="Number of future steps to project (default: 3)")
    p.add_argument("--pipeline", default=None, help="Limit projection to a single pipeline")
    p.add_argument("--json", dest="as_json", action="store_true", help="Output as JSON")
    return p


def _load_history(path: str) -> List[RunRecord]:
    p = Path(path)
    if not p.exists():
        return []
    raw = json.loads(p.read_text())
    return [
        RunRecord(
            pipeline=r["pipeline"],
            timestamp=r["timestamp"],
            level=r["level"],
            error_rate=float(r.get("error_rate", 0.0)),
            latency_ms=float(r.get("latency_ms", 0.0)),
            rows_processed=int(r.get("rows_processed", 0)),
        )
        for r in raw
    ]


def _format_result(result: ProjectionResult) -> str:
    lines = [f"  {result.summary()}"]
    for pt in result.points:
        lines.append(f"    step {pt.step}: {pt.summary()}")
    return "\n".join(lines)


def cmd_project(args: argparse.Namespace) -> None:
    records = _load_history(args.history_file)
    if not records:
        print("No history records found.", file=sys.stderr)
        return

    if args.pipeline:
        records = [r for r in records if r.pipeline == args.pipeline]

    results = project_all(records, steps=args.steps)

    if not results:
        print("Not enough data to project any pipelines.", file=sys.stderr)
        return

    if args.as_json:
        output = [
            {
                "pipeline": r.pipeline,
                "steps": r.steps,
                "degrading": r.degrading,
                "improving": r.improving,
                "points": [
                    {
                        "step": pt.step,
                        "projected_error_rate": pt.projected_error_rate,
                        "projected_latency_ms": pt.projected_latency_ms,
                    }
                    for pt in r.points
                ],
            }
            for r in results
        ]
        print(json.dumps(output, indent=2))
    else:
        print(f"Projections ({args.steps} steps ahead):")
        for result in results:
            print(_format_result(result))
