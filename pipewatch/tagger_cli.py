"""CLI command for tagging pipeline statuses using configured rules."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.tagger import TaggerConfig, TaggedPipeline, tag_all


def _build_tag_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch tag",
        description="Tag pipeline statuses using rule-based config.",
    )
    p.add_argument("statuses", help="Path to JSON file with pipeline statuses")
    p.add_argument("--config", required=True, help="Path to tagger config YAML")
    p.add_argument("--output", default="-", help="Output file path (default: stdout)")
    p.add_argument("--only-tagged", action="store_true", help="Only emit statuses that received at least one tag")
    return p


def _load_statuses(path: str) -> list[PipelineStatus]:
    data = json.loads(Path(path).read_text())
    return [
        PipelineStatus(
            pipeline_name=d["pipeline_name"],
            level=AlertLevel[d["level"]],
            message=d.get("message", ""),
            error_rate=d.get("error_rate", 0.0),
            latency_ms=d.get("latency_ms", 0.0),
        )
        for d in data
    ]


def cmd_tag(args: argparse.Namespace) -> None:
    import yaml

    raw = yaml.safe_load(Path(args.config).read_text())
    rules = raw.get("rules", [])
    cfg = TaggerConfig(rules=[
        __import__("pipewatch.tagger", fromlist=["TagRule"]).TagRule(**r)
        for r in rules
    ])

    statuses = _load_statuses(args.statuses)
    tagged: list[TaggedPipeline] = tag_all(statuses, cfg)

    if args.only_tagged:
        tagged = [t for t in tagged if t.tags]

    output = json.dumps([{"pipeline_name": t.status.pipeline_name, "level": t.status.level.name, "tags": t.tags} for t in tagged], indent=2)

    if args.output == "-":
        print(output)
    else:
        Path(args.output).write_text(output)
        print(f"Tagged {len(tagged)} pipeline(s) -> {args.output}", file=sys.stderr)
