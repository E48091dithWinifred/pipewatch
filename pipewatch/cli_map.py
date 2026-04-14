"""CLI sub-command: pipewatch map — display pipeline dependency information."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.mapper import build_map, affected_by, DependencyMap


def _build_map_parser(sub: "argparse._SubParsersAction") -> argparse.ArgumentParser:  # type: ignore[type-arg]
    p = sub.add_parser("map", help="Show pipeline dependency map")
    p.add_argument("edges_file", help="JSON file with {edges: [{upstream, downstream}]}")
    p.add_argument(
        "--failed",
        metavar="PIPELINE",
        default=None,
        help="Show pipelines affected by a failure in PIPELINE",
    )
    p.add_argument(
        "--roots",
        action="store_true",
        help="List root pipelines (no upstream dependencies)",
    )
    p.add_argument(
        "--leaves",
        action="store_true",
        help="List leaf pipelines (no downstream dependents)",
    )
    return p


def _load_map(path: str) -> DependencyMap:
    p = Path(path)
    if not p.exists():
        print(f"[error] edges file not found: {path}", file=sys.stderr)
        sys.exit(1)
    data = json.loads(p.read_text())
    return build_map(data.get("edges", []))


def cmd_map(args: argparse.Namespace) -> None:
    dep_map = _load_map(args.edges_file)

    if args.failed:
        downstream = affected_by(dep_map, args.failed)
        if not downstream:
            print(f"No pipelines affected by failure in '{args.failed}'.")
        else:
            print(f"Pipelines affected by failure in '{args.failed}':")
            for name in downstream:
                print(f"  - {name}")
        return

    if args.roots:
        roots = dep_map.roots()
        print("Root pipelines (no upstream):")
        for r in roots:
            print(f"  {r}")
        return

    if args.leaves:
        leaves = dep_map.leaves()
        print("Leaf pipelines (no downstream):")
        for lf in leaves:
            print(f"  {lf}")
        return

    # Default: print full map summary
    if not dep_map.nodes:
        print("No pipelines in dependency map.")
        return

    print(f"{'Pipeline':<30} {'Upstream':<30} {'Downstream'}")
    print("-" * 80)
    for name, node in sorted(dep_map.nodes.items()):
        up = ", ".join(node.upstream) or "-"
        down = ", ".join(node.downstream) or "-"
        print(f"{name:<30} {up:<30} {down}")
