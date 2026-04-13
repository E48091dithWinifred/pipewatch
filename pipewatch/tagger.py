"""Tag pipelines with custom labels based on configurable rules."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import AlertLevel, PipelineStatus


@dataclass
class TagRule:
    """A single rule that maps a condition to a tag string."""
    tag: str
    level: Optional[str] = None          # e.g. "CRITICAL", "WARNING", "OK"
    min_error_rate: Optional[float] = None
    max_error_rate: Optional[float] = None
    name_contains: Optional[str] = None


@dataclass
class TaggerConfig:
    rules: List[TagRule] = field(default_factory=list)


@dataclass
class TaggedPipeline:
    name: str
    level: str
    tags: List[str]
    error_rate: float


def _rule_matches(rule: TagRule, status: PipelineStatus) -> bool:
    """Return True if *all* specified conditions on *rule* match *status*."""
    if rule.level is not None:
        if status.level.name != rule.level.upper():
            return False

    error_rate = status.metrics.error_rate if status.metrics else 0.0

    if rule.min_error_rate is not None:
        if error_rate < rule.min_error_rate:
            return False

    if rule.max_error_rate is not None:
        if error_rate > rule.max_error_rate:
            return False

    if rule.name_contains is not None:
        if rule.name_contains.lower() not in status.pipeline_name.lower():
            return False

    return True


def tag_pipeline(status: PipelineStatus, config: TaggerConfig) -> TaggedPipeline:
    """Apply all matching rules and return a TaggedPipeline."""
    tags: List[str] = []
    for rule in config.rules:
        if _rule_matches(rule, status):
            if rule.tag not in tags:
                tags.append(rule.tag)

    error_rate = status.metrics.error_rate if status.metrics else 0.0
    return TaggedPipeline(
        name=status.pipeline_name,
        level=status.level.name,
        tags=tags,
        error_rate=error_rate,
    )


def tag_all(
    statuses: List[PipelineStatus], config: TaggerConfig
) -> List[TaggedPipeline]:
    """Tag every pipeline status in *statuses*."""
    return [tag_pipeline(s, config) for s in statuses]
