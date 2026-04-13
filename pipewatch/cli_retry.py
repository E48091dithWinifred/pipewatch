"""CLI sub-command: pipewatch retry — show and manage retry state."""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import List

from pipewatch.retrier import (
    RetryPolicy,
    _load_state,
    get_pending,
    resolve,
)

_DEFAULT_STATE = ".pipewatch/retry_state.json"


def _build_retry_parser(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("retry", help="Manage pipeline retry state")
    p.add_argument("--state-file", default=_DEFAULT_STATE)
    p.add_argument("--max-attempts", type=int, default=3)
    p.add_argument("--backoff", type=float, default=5.0)
    p.add_argument("--retry-on", nargs="+", default=["CRITICAL"])

    sub2 = p.add_subparsers(dest="retry_cmd")

    list_p = sub2.add_parser("list", help="List all retry entries")
    list_p.add_argument("--pending-only", action="store_true")

    resolve_p = sub2.add_parser("resolve", help="Mark a pipeline as resolved")
    resolve_p.add_argument("pipeline", help="Pipeline name to resolve")

    sub2.add_parser("clear", help="Clear all retry state")


def _make_policy(args: argparse.Namespace) -> RetryPolicy:
    return RetryPolicy(
        max_attempts=args.max_attempts,
        backoff_seconds=args.backoff,
        retry_on=args.retry_on,
    )


def cmd_retry(args: argparse.Namespace) -> int:
    state_file = args.state_file
    policy = _make_policy(args)

    if args.retry_cmd == "list" or args.retry_cmd is None:
        if not os.path.exists(state_file):
            print("No retry state found.")
            return 0 getattr(args, "pending_only", False):
            entries = get_pending(state_file, policy)
        else:
            entries = list(_load_state(state_file).values())
        if not entries:
            print("No entries.")
            return 0
        for e in entries:
            status = "resolved" if e.resolved else "pending"
            print(f"  {e.pipeline:30s}  level={e.level}  attempts={e.attempts}  [{status}]")
        return 0

    if args.retry_cmd == "resolve":
        entry = resolve(args.pipeline, state_file)
        if entry is None:
            print(f"Pipeline '{args.pipeline}' not found in retry state.", file=sys.stderr)
            return 1
        print(f"Resolved: {args.pipeline}")
        return 0

    if args.retry_cmd == "clear":
        if os.path.exists(state_file):
            os.remove(state_file)
        print("Retry state cleared.")
        return 0

    print("Unknown retry sub-command.", file=sys.stderr)
    return 1
