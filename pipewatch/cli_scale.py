"""cli_scale.py — CLI command to scale pipeline metric values to 0–1."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.scaler import ScalerConfig, ScaledStatus, scale_all


def _build_scale_parser(parent: argparse._SubParsersAction | None = None) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    kwargs = dict(description="Scale pipeline metrics to a 0–1 range.")
    parser = parent.add_parser("scale", **kwargs) if parent else argparse.ArgumentParser(**kwargs)
    parser.add_argument("input", help="JSON file with pipeline statuses")
    parser.add_argument("--max-error-rate", type=float, default=1.0, metavar="RATE")
    parser.add_argument("--max-latency-ms", type=float, default=10_000.0, metavar="MS")
    parser.add_argument("--output", choices=["table", "json"], default="table")
    return parser


def _load_statuses(path: str) -> List[PipelineStatus]:
    p = Path(path)
    if not p.exists():
        print(f"[error] file not found: {path}", file=sys.stderr)
        return []
    raw = json.loads(p.read_text())
    statuses: List[PipelineStatus] = []
    for item in raw:
        statuses.append(
            PipelineStatus(
                pipeline_name=item["pipeline_name"],
                level=AlertLevel(item["level"]),
                error_rate=item.get("error_rate", 0.0),
                latency_ms=item.get("latency_ms", 0.0),
                message=item.get("message", ""),
            )
        )
    return statuses


def _format_row(scaled: ScaledStatus) -> str:
    return (
        f"{scaled.name:<30} {scaled.level:<10} "
        f"{scaled.error_rate_scaled:>8.3f}   "
        f"{scaled.latency_scaled:>10.3f}   "
        f"{scaled.composite_score:>9.3f}"
    )


def cmd_scale(args: argparse.Namespace) -> None:
    statuses = _load_statuses(args.input)
    if not statuses:
        print("No statuses to scale.")
        return

    try:
        cfg = ScalerConfig(
            max_error_rate=args.max_error_rate,
            max_latency_ms=args.max_latency_ms,
        )
    except ValueError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)

    results = scale_all(statuses, cfg)

    if args.output == "json":
        payload = [
            {
                "name": r.name,
                "level": r.level,
                "error_rate_scaled": r.error_rate_scaled,
                "latency_scaled": r.latency_scaled,
                "composite_score": r.composite_score,
            }
            for r in results
        ]
        print(json.dumps(payload, indent=2))
    else:
        header = f"{'PIPELINE':<30} {'LEVEL':<10} {'ERR_SCALED':>10} {'LAT_SCALED':>12} {'COMPOSITE':>11}"
        print(header)
        print("-" * len(header))
        for r in results:
            print(_format_row(r))


def main() -> None:  # pragma: no cover
    parser = _build_scale_parser()
    cmd_scale(parser.parse_args())


if __name__ == "__main__":  # pragma: no cover
    main()
