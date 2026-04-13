"""CLI sub-commands for managing alert throttle state."""
from __future__ import annotations

import argparse
import json
import os
import sys

from pipewatch.throttler import (
    ThrottleConfig,
    _load_state,
    is_throttled,
    record_fire,
    reset_throttle,
)

DEFAULT_STATE = ".pipewatch/throttle_state.json"


def _build_throttle_parser(parent: argparse._SubParsersAction) -> None:
    p = parent.add_parser("throttle", help="Manage alert throttle state")
    sub = p.add_subparsers(dest="throttle_cmd", required=True)

    ls = sub.add_parser("list", help="List active throttle entries")
    ls.add_argument("--state", default=DEFAULT_STATE)

    chk = sub.add_parser("check", help="Check if an alert is throttled")
    chk.add_argument("pipeline")
    chk.add_argument("level")
    chk.add_argument("--cooldown", type=int, default=30)
    chk.add_argument("--state", default=DEFAULT_STATE)

    rst = sub.add_parser("reset", help="Reset throttle for a pipeline+level")
    rst.add_argument("pipeline")
    rst.add_argument("level")
    rst.add_argument("--state", default=DEFAULT_STATE)


def cmd_throttle(args: argparse.Namespace) -> int:
    if args.throttle_cmd == "list":
        if not os.path.exists(args.state):
            print("No throttle state found.")
            return 0
        state = _load_state(args.state)
        if not state:
            print("No active throttle entries.")
            return 0
        for key, entry in state.items():
            print(
                f"  {entry.pipeline:<30} {entry.level:<10} "
                f"fires={entry.fire_count}  last={entry.last_fired}"
            )
        return 0

    if args.throttle_cmd == "check":
        cfg = ThrottleConfig(cooldown_minutes=args.cooldown, state_path=args.state)
        throttled = is_throttled(args.pipeline, args.level, cfg)
        status = "THROTTLED" if throttled else "ALLOWED"
        print(f"{args.pipeline} [{args.level}]: {status}")
        return 0 if not throttled else 1

    if args.throttle_cmd == "reset":
        cfg = ThrottleConfig(state_path=args.state)
        removed = reset_throttle(args.pipeline, args.level, cfg)
        if removed:
            print(f"Throttle reset for {args.pipeline} [{args.level}].")
        else:
            print(f"No throttle entry found for {args.pipeline} [{args.level}].")
        return 0

    return 1
