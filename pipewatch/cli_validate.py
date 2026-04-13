"""CLI sub-command: pipewatch validate — validate pipeline statuses against rules."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.validator import ValidationRule, validate_all


def _build_validate_parser(sub: argparse._SubParsersAction) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    p = sub.add_parser("validate", help="Validate pipeline statuses against rules")
    p.add_argument("statuses", help="JSON file of pipeline statuses")
    p.add_argument(
        "--max-error-rate",
        type=float,
        default=None,
        metavar="RATE",
        help="Maximum allowed error rate (0-1)",
    )
    p.add_argument(
        "--max-latency-ms",
        type=float,
        default=None,
        metavar="MS",
        help="Maximum allowed latency in milliseconds",
    )
    p.add_argument(
        "--forbid-level",
        action="append",
        default=[],
        dest="forbidden_levels",
        metavar="LEVEL",
        help="Forbidden alert level (may be repeated)",
    )
    p.add_argument(
        "--fail-fast",
        action="store_true",
        help="Exit with code 1 if any violations are found",
    )
    return p


def _load_statuses(path: str) -> List[PipelineStatus]:
    data = json.loads(Path(path).read_text())
    statuses = []
    for item in data:
        statuses.append(
            PipelineStatus(
                pipeline_name=item["pipeline_name"],
                level=AlertLevel(item["level"]),
                message=item.get("message", ""),
                error_rate=item.get("error_rate"),
                latency_ms=item.get("latency_ms"),
            )
        )
    return statuses


def cmd_validate(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.statuses)
    rule = ValidationRule(
        name="cli-rule",
        max_error_rate=args.max_error_rate,
        max_latency_ms=args.max_latency_ms,
        forbidden_levels=args.forbidden_levels,
    )
    reports = validate_all(statuses, [rule])

    any_violations = False
    for report in reports:
        if report.violations:
            any_violations = True
            for v in report.violations:
                print(f"FAIL  {v.summary}")
        else:
            print(f"PASS  [{report.pipeline}] all rules satisfied")

    if args.fail_fast and any_violations:
        sys.exit(1)
