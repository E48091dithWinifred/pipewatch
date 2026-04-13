"""Export pipeline metrics and status reports to various output formats."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import asdict
from typing import List, Literal

from pipewatch.checker import PipelineStatus
from pipewatch.digest import Digest

ExportFormat = Literal["json", "csv", "text"]


def _status_to_dict(status: PipelineStatus) -> dict:
    return {
        "pipeline": status.pipeline_name,
        "level": status.level.value,
        "message": status.message,
        "error_rate": status.error_rate,
        "latency_ms": status.latency_ms,
    }


def export_statuses(statuses: List[PipelineStatus], fmt: ExportFormat) -> str:
    """Serialize a list of PipelineStatus objects to the requested format."""
    if fmt == "json":
        return json.dumps([_status_to_dict(s) for s in statuses], indent=2)

    if fmt == "csv":
        buf = io.StringIO()
        fieldnames = ["pipeline", "level", "message", "error_rate", "latency_ms"]
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        for s in statuses:
            writer.writerow(_status_to_dict(s))
        return buf.getvalue()

    if fmt == "text":
        lines = []
        for s in statuses:
            lines.append(
                f"{s.pipeline_name}\t{s.level.value}\t"
                f"error_rate={s.error_rate:.4f}\tlatency_ms={s.latency_ms:.1f}\t{s.message}"
            )
        return "\n".join(lines)

    raise ValueError(f"Unsupported export format: {fmt}")


def export_digest(digest: Digest, fmt: ExportFormat) -> str:
    """Serialize a Digest to the requested format."""
    data = {
        "generated_at": digest.generated_at,
        "total_runs": digest.total_runs,
        "ok_count": digest.ok_count,
        "warning_count": digest.warning_count,
        "critical_count": digest.critical_count,
        "pipelines": [
            {
                "pipeline": pd.pipeline_name,
                "health_score": pd.health_score,
                "avg_error_rate": pd.avg_error_rate,
                "avg_latency_ms": pd.avg_latency_ms,
                "most_critical": pd.most_critical,
            }
            for pd in digest.pipelines
        ],
    }

    if fmt == "json":
        return json.dumps(data, indent=2)

    if fmt == "text":
        lines = [
            f"Digest generated_at={data['generated_at']}",
            f"  total_runs={data['total_runs']}  ok={data['ok_count']}  "
            f"warning={data['warning_count']}  critical={data['critical_count']}",
        ]
        for p in data["pipelines"]:
            lines.append(
                f"  {p['pipeline']}  health={p['health_score']:.2f}  "
                f"avg_error_rate={p['avg_error_rate']:.4f}  "
                f"avg_latency_ms={p['avg_latency_ms']:.1f}  "
                f"most_critical={p['most_critical']}"
            )
        return "\n".join(lines)

    raise ValueError(f"Unsupported export format: {fmt}")
