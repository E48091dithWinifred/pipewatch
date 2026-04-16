"""Filter pipelines by tags applied via the tagger module."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.tagger import TaggedPipeline


@dataclass
class TagFilterConfig:
    require_all: List[str] = field(default_factory=list)
    require_any: List[str] = field(default_factory=list)
    exclude: List[str] = field(default_factory=list)


@dataclass
class TagFilterResult:
    matched: List[TaggedPipeline]
    dropped: List[TaggedPipeline]

    @property
    def matched_count(self) -> int:
        return len(self.matched)

    @property
    def dropped_count(self) -> int:
        return len(self.dropped)

    def summary(self) -> str:
        return f"matched={self.matched_count} dropped={self.dropped_count}"


def _passes(tagged: TaggedPipeline, cfg: TagFilterConfig) -> bool:
    tags = set(tagged.tags)
    if cfg.require_all and not set(cfg.require_all).issubset(tags):
        return False
    if cfg.require_any and tags.isdisjoint(cfg.require_any):
        return False
    if cfg.exclude and not tags.isdisjoint(cfg.exclude):
        return False
    return True


def filter_by_tags(
    tagged_pipelines: List[TaggedPipeline],
    cfg: Optional[TagFilterConfig] = None,
) -> TagFilterResult:
    if cfg is None:
        cfg = TagFilterConfig()
    matched = [t for t in tagged_pipelines if _passes(t, cfg)]
    dropped = [t for t in tagged_pipelines if not _passes(t, cfg)]
    return TagFilterResult(matched=matched, dropped=dropped)
