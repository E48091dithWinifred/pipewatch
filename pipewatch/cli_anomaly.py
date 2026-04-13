"""CLI sub-command: pipewatch anomaly — detect metric anomalies in run history."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.anomaly import detect_all_anomalies, detect_anomaly
from pipewatch.history import RunRecord, _load_records


def _build_anomaly_parser(sub: argparse._SubParsersAction) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    p = sub.add_parser("anomaly", help="Detect anomalies in pipeline run history")
    p.add_argument("--history", default=".pipewatch_history.json", help="Path to history file")
    p.add_argument("--pipeline", default=None, help="Limit to a single pipeline name")
    p.add_argument("--metric", choices=["error_rate", "latency_ms"], default=None,
                   help="Limit to a single metric")
    p.add_argument("--threshold", type=float, default=2.5,
                   help="Z-score threshold for anomaly (default: 2.5)")
    p.add_argument("--json", dest="output_json", action="store_true",
                   help="Output results as JSON")
    return p


def _load_history(path: str) -> List[RunRecord]:
    p = Path(path)
    if not p.exists():
        return []
    return _load_records(p)


def cmd_anomaly(args: argparse.Namespace) -> int:
    records = _load_history(args.history)
    if not records:
        print("No history records found.", file=sys.stderr)
        return 1

    metrics = [args.metric] if args.metric else ["error_rate", "latency_ms"]

    if args.pipeline:
        results = []
        for metric in metrics:
            r = detect_anomaly(args.pipeline, records, metric, args.threshold)
            if r is not None and r.is_anomaly:
                results.append(r)
    else:
        results = detect_all_anomalies(records, threshold_z=args.threshold)
        if args.metric:
            results = [r for r in results if r.metric == args.metric]

    if not results:
        print("No anomalies detected.")
        return 0

    if args.output_json:
        output = [
            {
                "pipeline": r.pipeline_name,
                "metric": r.metric,
                "current_value": r.current_value,
                "mean": r.mean,
                "std_dev": r.std_dev,
                "z_score": r.z_score,
            }
            for r in results
        ]
        print(json.dumps(output, indent=2))
    else:
        for r in results:
            print(r.summary)

    return 0
