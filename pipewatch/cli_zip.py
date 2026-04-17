"""CLI command for zipping two status files and reporting differences."""
import argparse
import json
import sys
from typing import List
from pipewatch.checker import PipelineStatus, AlertLevel
from pipewatch.zipper import zip_statuses


def _build_zip_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Zip two pipeline status files by name")
    p.add_argument("left", help="Path to left status JSON file")
    p.add_argument("right", help="Path to right status JSON file")
    p.add_argument(
        "--only-diff",
        action="store_true",
        help="Only show pairs where levels differ or one side is missing",
    )
    return p


def _load_statuses(path: str) -> List[PipelineStatus]:
    try:
        with open(path) as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"[error] file not found: {path}", file=sys.stderr)
        return []
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


def cmd_zip(args: argparse.Namespace) -> None:
    left = _load_statuses(args.left)
    right = _load_statuses(args.right)
    result = zip_statuses(left, right)
    print(result.summary())
    for pair in result.pairs:
        left_level = pair.left.level.value if pair.left else "missing"
        right_level = pair.right.level.value if pair.right else "missing"
        if args.only_diff and left_level == right_level:
            continue
        marker = "~~" if left_level != right_level else "=="
        print(f"  {marker} {pair.name}: {left_level} -> {right_level}")


def main() -> None:
    parser = _build_zip_parser()
    args = parser.parse_args()
    cmd_zip(args)


if __name__ == "__main__":
    main()
