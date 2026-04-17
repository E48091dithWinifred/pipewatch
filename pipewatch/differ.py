"""Differ: compute field-level diffs between two pipeline status snapshots."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.checker import PipelineStatus


@dataclass
class FieldDiff:
    field: str
    before: object
    after: object

    @property
    def changed(self) -> bool:
        return self.before != self.after

    def summary(self) -> str:
        return f"{self.field}: {self.before!r} -> {self.after!r}"


@dataclass
class DiffResult:
    pipeline: str
    diffs: List[FieldDiff]

    @property
    def has_changes(self) -> bool:
        return any(d.changed for d in self.diffs)

    @property
    def changed_fields(self) -> List[str]:
        return [d.field for d in self.diffs if d.changed]

    def summary(self) -> str:
        if not self.has_changes:
            return f"{self.pipeline}: no changes"
        parts = "; ".join(d.summary() for d in self.diffs if d.changed)
        return f"{self.pipeline}: {parts}"


_TRACKED_FIELDS = ["level", "message", "error_rate", "latency_ms"]


def diff_status(before: PipelineStatus, after: PipelineStatus) -> DiffResult:
    """Return a DiffResult comparing two PipelineStatus objects by tracked fields."""
    diffs = []
    for field in _TRACKED_FIELDS:
        b = getattr(before, field, None)
        a = getattr(after, field, None)
        diffs.append(FieldDiff(field=field, before=b, after=a))
    return DiffResult(pipeline=after.pipeline, diffs=diffs)


def diff_all(
    before: List[PipelineStatus], after: List[PipelineStatus]
) -> List[DiffResult]:
    """Diff two lists of PipelineStatus by pipeline name; only returns changed pipelines."""
    before_map = {s.pipeline: s for s in before}
    after_map = {s.pipeline: s for s in after}
    results = []
    for name, a in after_map.items():
        b = before_map.get(name)
        if b is None:
            continue
        result = diff_status(b, a)
        if result.has_changes:
            results.append(result)
    return results
