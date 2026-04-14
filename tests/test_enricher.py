"""Tests for pipewatch.enricher."""
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.enricher import (
    EnrichmentConfig,
    EnrichedStatus,
    enrich_status,
    enrich_all,
)


def _make_status(
    name: str = "etl.orders",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.0,
    latency_ms: float = 100.0,
) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message="ok",
        error_rate=error_rate,
        latency_ms=latency_ms,
    )


@pytest.fixture
def cfg() -> EnrichmentConfig:
    return EnrichmentConfig(
        prefix_tags={"etl.": ["etl", "batch"], "stream.": ["streaming"]},
        owner_map={"etl.": "data-eng", "stream.": "platform"},
        environment="staging",
    )


def test_enrich_status_returns_enriched_instance(cfg):
    s = _make_status()
    result = enrich_status(s, cfg)
    assert isinstance(result, EnrichedStatus)


def test_enrich_status_name_passthrough(cfg):
    s = _make_status(name="etl.orders")
    result = enrich_status(s, cfg)
    assert result.name == "etl.orders"


def test_enrich_status_level_passthrough(cfg):
    s = _make_status(level=AlertLevel.WARNING)
    result = enrich_status(s, cfg)
    assert result.level == AlertLevel.WARNING


def test_enrich_status_tags_matched_by_prefix(cfg):
    s = _make_status(name="etl.orders")
    result = enrich_status(s, cfg)
    assert "etl" in result.tags
    assert "batch" in result.tags


def test_enrich_status_tags_empty_for_unknown_prefix(cfg):
    s = _make_status(name="unknown.pipeline")
    result = enrich_status(s, cfg)
    assert result.tags == []


def test_enrich_status_owner_matched(cfg):
    s = _make_status(name="etl.orders")
    result = enrich_status(s, cfg)
    assert result.owner == "data-eng"


def test_enrich_status_owner_none_for_unknown(cfg):
    s = _make_status(name="unknown.pipeline")
    result = enrich_status(s, cfg)
    assert result.owner is None


def test_enrich_status_environment_set(cfg):
    s = _make_status()
    result = enrich_status(s, cfg)
    assert result.environment == "staging"


def test_enrich_all_returns_correct_count(cfg):
    statuses = [_make_status(name=f"etl.p{i}") for i in range(4)]
    results = enrich_all(statuses, cfg)
    assert len(results) == 4


def test_enrich_all_empty_list(cfg):
    assert enrich_all([], cfg) == []


def test_as_dict_contains_expected_keys(cfg):
    s = _make_status(name="stream.events", error_rate=0.05, latency_ms=250.0)
    result = enrich_status(s, cfg)
    d = result.as_dict()
    assert d["name"] == "stream.events"
    assert d["environment"] == "staging"
    assert d["owner"] == "platform"
    assert "streaming" in d["tags"]
    assert d["error_rate"] == pytest.approx(0.05)
    assert d["latency_ms"] == pytest.approx(250.0)
