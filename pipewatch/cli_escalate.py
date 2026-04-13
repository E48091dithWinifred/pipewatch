"""CLI sub-command: pipewatch escalate

Reads a list of pipeline statuses (JSON), applies escalation rules from a
YAML config, and prints the (possibly escalated) results.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

import yaml

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.escalator import (
    EscalationConfig,
    EscalationRule,
    EscalationState,
    escalate,
)


def _build_escalate_parser(sub: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = sub.add_parser("escalate", help="Apply escalation rules to pipeline statuses")
    p.add_argument("statuses", help="JSON file with pipeline statuses")
    p.add_argument("--config", required=True, help="YAML escalation config file")
    p.add_argument(
        "--state", default=None, help="Optional JSON file to persist escalation state"
    )
    p.set_defaults(func=cmd_escalate)


def _load_statuses(path: str) -> List[PipelineStatus]:
    data = json.loads(Path(path).read_text())
    results = []
    for item in data:
        results.append(
            PipelineStatus(
                pipeline=item["pipeline"],
                level=AlertLevel[item["level"].upper()],
                message=item.get("message", ""),
                error_rate=item.get("error_rate", 0.0),
                latency_ms=item.get("latency_ms", 0.0),
            )
        )
    return results


def _load_config(path: str) -> EscalationConfig:
    raw = yaml.safe_load(Path(path).read_text())
    rules = [
        EscalationRule(
            from_level=r["from_level"],
            to_level=r["to_level"],
            after_runs=int(r.get("after_runs", 3)),
        )
        for r in raw.get("rules", [])
    ]
    return EscalationConfig(rules=rules)


def cmd_escalate(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.statuses)
    config = _load_config(args.config)
    state = EscalationState()

    results = []
    for status in statuses:
        escalated = escalate(status, config, state)
        results.append(
            {
                "pipeline": escalated.pipeline,
                "level": escalated.level.name,
                "message": escalated.message,
                "error_rate": escalated.error_rate,
                "latency_ms": escalated.latency_ms,
            }
        )

    json.dump(results, sys.stdout, indent=2)
    sys.stdout.write("\n")
