"""Output formatter: render pipeline statuses in table, json, or csv format."""
from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from typing import List, Literal

from pipewatch.checker import PipelineStatus

FormatType = Literal["table", "json", "csv"]

_COLUMN_WIDTHS = {"name": 28, "level": 10, "error_rate": 12, "latency_ms": 12, "message": 30}


@dataclass(frozen=True)
class FormatterConfig:
    fmt: FormatType = "table"
    show_message: bool = True
    max_rows: int = 0  # 0 = unlimited


def _header_row() -> str:
    return (
        f"{'PIPELINE':<28}  {'LEVEL':<10}  {'ERR_RATE':>10}  {'LATENCY_MS':>10}  MESSAGE"
    )


def _separator() -> str:
    return "-" * 80


def _status_row(s: PipelineStatus, show_message: bool) -> str:
    err = f"{s.error_rate:.2%}" if s.error_rate is not None else "n/a"
    lat = f"{s.latency_ms:.1f}" if s.latency_ms is not None else "n/a"
    msg = (s.message or "") if show_message else ""
    return f"{s.pipeline_name:<28}  {s.level.value:<10}  {err:>10}  {lat:>10}  {msg}"


def format_table(statuses: List[PipelineStatus], cfg: FormatterConfig) -> str:
    rows = statuses[: cfg.max_rows] if cfg.max_rows else statuses
    lines = [_header_row(), _separator()]
    for s in rows:
        lines.append(_status_row(s, cfg.show_message))
    lines.append(_separator())
    lines.append(f"Total: {len(rows)} pipeline(s)")
    return "\n".join(lines)


def format_json(statuses: List[PipelineStatus], cfg: FormatterConfig) -> str:
    rows = statuses[: cfg.max_rows] if cfg.max_rows else statuses
    data = [
        {
            "pipeline_name": s.pipeline_name,
            "level": s.level.value,
            "error_rate": s.error_rate,
            "latency_ms": s.latency_ms,
            "message": s.message if cfg.show_message else None,
        }
        for s in rows
    ]
    return json.dumps(data, indent=2)


def format_csv(statuses: List[PipelineStatus], cfg: FormatterConfig) -> str:
    rows = statuses[: cfg.max_rows] if cfg.max_rows else statuses
    buf = io.StringIO()
    fieldnames = ["pipeline_name", "level", "error_rate", "latency_ms"]
    if cfg.show_message:
        fieldnames.append("message")
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for s in rows:
        row = {
            "pipeline_name": s.pipeline_name,
            "level": s.level.value,
            "error_rate": s.error_rate,
            "latency_ms": s.latency_ms,
        }
        if cfg.show_message:
            row["message"] = s.message or ""
        writer.writerow(row)
    return buf.getvalue()


def render(statuses: List[PipelineStatus], cfg: FormatterConfig | None = None) -> str:
    """Dispatch to the appropriate formatter based on cfg.fmt."""
    if cfg is None:
        cfg = FormatterConfig()
    if cfg.fmt == "json":
        return format_json(statuses, cfg)
    if cfg.fmt == "csv":
        return format_csv(statuses, cfg)
    return format_table(statuses, cfg)
