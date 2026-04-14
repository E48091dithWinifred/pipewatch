"""Enricher: attach contextual metadata to pipeline statuses."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.checker import PipelineStatus, AlertLevel


@dataclass
class EnrichmentConfig:
    """Rules that map pipeline name prefixes to metadata tags."""
    prefix_tags: Dict[str, List[str]] = field(default_factory=dict)
    owner_map: Dict[str, str] = field(default_factory=dict)  # prefix -> owner
    environment: str = "production"


@dataclass
class EnrichedStatus:
    """A PipelineStatus decorated with extra contextual fields."""
    status: PipelineStatus
    tags: List[str]
    owner: Optional[str]
    environment: str

    # convenience pass-throughs
    @property
    def name(self) -> str:
        return self.status.pipeline_name

    @property
    def level(self) -> AlertLevel:
        return self.status.level

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "level": self.level.value,
            "tags": self.tags,
            "owner": self.owner,
            "environment": self.environment,
            "error_rate": self.status.error_rate,
            "latency_ms": self.status.latency_ms,
        }


def _collect_tags(name: str, prefix_tags: Dict[str, List[str]]) -> List[str]:
    tags: List[str] = []
    for prefix, t in prefix_tags.items():
        if name.startswith(prefix):
            tags.extend(t)
    return tags


def _find_owner(name: str, owner_map: Dict[str, str]) -> Optional[str]:
    for prefix, owner in owner_map.items():
        if name.startswith(prefix):
            return owner
    return None


def enrich_status(status: PipelineStatus, cfg: EnrichmentConfig) -> EnrichedStatus:
    """Return an EnrichedStatus for a single pipeline status."""
    return EnrichedStatus(
        status=status,
        tags=_collect_tags(status.pipeline_name, cfg.prefix_tags),
        owner=_find_owner(status.pipeline_name, cfg.owner_map),
        environment=cfg.environment,
    )


def enrich_all(
    statuses: List[PipelineStatus], cfg: EnrichmentConfig
) -> List[EnrichedStatus]:
    """Enrich a list of pipeline statuses."""
    return [enrich_status(s, cfg) for s in statuses]
