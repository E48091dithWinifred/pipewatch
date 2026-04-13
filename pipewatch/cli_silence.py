"""CLI sub-command: pipewatch silence — list or evaluate silence rules."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.silencer import SilenceRule, SilencerConfig, apply_silencer


def _build_silence_parser(sub: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = sub.add_parser("silence", help="Evaluate silence rules against pipeline statuses")
    p.add_argument("statuses", metavar="FILE", help="JSON file with pipeline statuses")
    p.add_argument(
        "--rules",
        metavar="FILE",
        required=True,
        help="JSON file with silence rules",
    )
    p.add_argument(
        "--show-silenced",
        action="store_true",
        help="Print silenced pipelines instead of active ones",
    )
    return p


def _load_statuses(path: str) -> list[PipelineStatus]:
    data = json.loads(Path(path).read_text())
    return [
        PipelineStatus(
            pipeline_name=d["pipeline_name"],
            level=AlertLevel(d["level"]),
            message=d.get("message", ""),
            error_rate=d.get("error_rate", 0.0),
            latency_ms=d.get("latency_ms", 0.0),
        )
        for d in data
    ]


def _load_rules(path: str) -> SilencerConfig:
    data = json.loads(Path(path).read_text())
    rules = [
        SilenceRule(
            pipeline_name=r["pipeline_name"],
            reason=r.get("reason", ""),
            until=r.get("until"),
            levels=r.get("levels", []),
        )
        for r in data
    ]
    return SilencerConfig(rules=rules)


def cmd_silence(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.statuses)
    config = _load_rules(args.rules)
    active, silenced = apply_silencer(statuses, config)

    target = silenced if args.show_silenced else active
    label = "SILENCED" if args.show_silenced else "ACTIVE"

    if not target:
        print(f"[silence] No {label.lower()} pipelines.")
        return

    print(f"[silence] {label} pipelines ({len(target)}):")
    for s in target:
        print(f"  {s.pipeline_name:30s}  {s.level.value.upper():8s}  {s.message}")
