"""CLI sub-command: enrich — attach metadata to pipeline statuses."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.enricher import EnrichmentConfig, enrich_all


def _build_enrich_parser(sub: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = sub.add_parser("enrich", help="Attach metadata tags and ownership to statuses")
    p.add_argument("input", help="JSON file of pipeline statuses")
    p.add_argument(
        "--prefix-tag",
        metavar="PREFIX:TAG",
        action="append",
        default=[],
        dest="prefix_tags",
        help="Map a name prefix to a tag (repeatable)",
    )
    p.add_argument(
        "--owner",
        metavar="PREFIX:OWNER",
        action="append",
        default=[],
        dest="owners",
        help="Map a name prefix to an owner (repeatable)",
    )
    p.add_argument("--env", default="production", help="Environment label")
    p.add_argument("--output", default="-", help="Output file (default: stdout)")
    return p


def _load_statuses(path: str) -> List[PipelineStatus]:
    p = Path(path)
    if not p.exists():
        return []
    raw = json.loads(p.read_text())
    result = []
    for item in raw:
        result.append(
            PipelineStatus(
                pipeline_name=item["name"],
                level=AlertLevel(item["level"]),
                message=item.get("message", ""),
                error_rate=item.get("error_rate", 0.0),
                latency_ms=item.get("latency_ms", 0.0),
            )
        )
    return result


def _parse_pairs(items: List[str]) -> dict:
    """Parse 'PREFIX:VALUE' strings into a dict (last write wins per prefix)."""
    out: dict = {}
    for item in items:
        if ":" not in item:
            continue
        prefix, _, value = item.partition(":")
        out.setdefault(prefix, []).append(value) if False else None
        out[prefix] = value
    return out


def cmd_enrich(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.input)

    prefix_tags: dict = {}
    for item in args.prefix_tags:
        prefix, _, tag = item.partition(":")
        prefix_tags.setdefault(prefix, []).append(tag)

    owners = _parse_pairs(args.owners)

    cfg = EnrichmentConfig(
        prefix_tags=prefix_tags,
        owner_map=owners,
        environment=args.env,
    )

    enriched = enrich_all(statuses, cfg)
    output = json.dumps([e.as_dict() for e in enriched], indent=2)

    if args.output == "-":
        print(output)
    else:
        Path(args.output).write_text(output)
        print(f"Wrote {len(enriched)} enriched statuses to {args.output}")
