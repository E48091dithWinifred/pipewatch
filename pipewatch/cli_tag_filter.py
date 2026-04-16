"""CLI command: pipewatch tag-filter — filter tagged pipeline statuses by tag criteria."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path
from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.tagger import TaggedPipeline, TaggerConfig, tag_pipeline
from pipewatch.tagger_filter import TagFilterConfig, filter_by_tags


def _build_tag_filter_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch tag-filter",
        description="Filter pipelines by tag criteria.",
    )
    p.add_argument("input", help="JSON file with pipeline statuses")
    p.add_argument("--tagger-config", help="YAML tagger config file")
    p.add_argument("--require-all", nargs="*", default=[], metavar="TAG")
    p.add_argument("--require-any", nargs="*", default=[], metavar="TAG")
    p.add_argument("--exclude", nargs="*", default=[], metavar="TAG")
    p.add_argument("--summary", action="store_true", help="Print summary only")
    return p


def _load_statuses(path: str) -> list[PipelineStatus]:
    data = json.loads(Path(path).read_text())
    out = []
    for item in data:
        out.append(
            PipelineStatus(
                pipeline_name=item["pipeline_name"],
                level=AlertLevel[item["level"]],
                message=item.get("message", ""),
                error_rate=item.get("error_rate", 0.0),
                latency_ms=item.get("latency_ms", 0.0),
            )
        )
    return out


def cmd_tag_filter(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.input)

    tagger_cfg = None
    if args.tagger_config:
        import yaml
        raw = yaml.safe_load(Path(args.tagger_config).read_text())
        from pipewatch.tagger import TagRule
        rules = [TagRule(**r) for r in raw.get("rules", [])]
        tagger_cfg = TaggerConfig(rules=rules)

    tagged = [tag_pipeline(s, tagger_cfg) for s in statuses]

    filter_cfg = TagFilterConfig(
        require_all=args.require_all or [],
        require_any=args.require_any or [],
        exclude=args.exclude or [],
    )
    result = filter_by_tags(tagged, filter_cfg)

    if args.summary:
        print(result.summary())
        return

    for t in result.matched:
        tags_str = ", ".join(t.tags) if t.tags else "(none)"
        print(f"{t.status.pipeline_name} [{t.status.level.name}] tags={tags_str}")


def main() -> None:
    parser = _build_tag_filter_parser()
    args = parser.parse_args()
    cmd_tag_filter(args)


if __name__ == "__main__":
    main()
