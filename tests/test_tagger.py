"""Tests for pipewatch.tagger."""
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.metrics import PipelineMetrics
from pipewatch.tagger import (
    TagRule,
    TaggerConfig,
    TaggedPipeline,
    _rule_matches,
    tag_pipeline,
    tag_all,
)


def _make_status(
    name: str = "pipe",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.0,
) -> PipelineStatus:
    metrics = PipelineMetrics(
        pipeline_name=name,
        total_records=1000,
        failed_records=int(error_rate * 1000),
        duration_seconds=10.0,
    )
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message="ok",
        metrics=metrics,
    )


# ---------------------------------------------------------------------------
# _rule_matches
# ---------------------------------------------------------------------------

def test_rule_matches_level_ok():
    rule = TagRule(tag="healthy", level="OK")
    assert _rule_matches(rule, _make_status(level=AlertLevel.OK))


def test_rule_no_match_wrong_level():
    rule = TagRule(tag="alert", level="CRITICAL")
    assert not _rule_matches(rule, _make_status(level=AlertLevel.OK))


def test_rule_matches_min_error_rate():
    rule = TagRule(tag="flaky", min_error_rate=0.05)
    assert _rule_matches(rule, _make_status(error_rate=0.10))
    assert not _rule_matches(rule, _make_status(error_rate=0.01))


def test_rule_matches_max_error_rate():
    rule = TagRule(tag="clean", max_error_rate=0.02)
    assert _rule_matches(rule, _make_status(error_rate=0.01))
    assert not _rule_matches(rule, _make_status(error_rate=0.05))


def test_rule_matches_name_contains():
    rule = TagRule(tag="ingest", name_contains="ingest")
    assert _rule_matches(rule, _make_status(name="ingest_daily"))
    assert not _rule_matches(rule, _make_status(name="transform_weekly"))


def test_rule_all_conditions_must_match():
    rule = TagRule(tag="critical-flaky", level="CRITICAL", min_error_rate=0.10)
    # level matches but error rate too low
    assert not _rule_matches(
        rule, _make_status(level=AlertLevel.CRITICAL, error_rate=0.05)
    )
    # both match
    assert _rule_matches(
        rule, _make_status(level=AlertLevel.CRITICAL, error_rate=0.15)
    )


# ---------------------------------------------------------------------------
# tag_pipeline
# ---------------------------------------------------------------------------

def test_tag_pipeline_no_rules_returns_empty_tags():
    config = TaggerConfig(rules=[])
    result = tag_pipeline(_make_status(), config)
    assert result.tags == []


def test_tag_pipeline_single_matching_rule():
    config = TaggerConfig(rules=[TagRule(tag="ok-pipe", level="OK")])
    result = tag_pipeline(_make_status(level=AlertLevel.OK), config)
    assert "ok-pipe" in result.tags


def test_tag_pipeline_multiple_rules_multiple_tags():
    config = TaggerConfig(
        rules=[
            TagRule(tag="monitored", name_contains="pipe"),
            TagRule(tag="critical", level="CRITICAL"),
        ]
    )
    result = tag_pipeline(
        _make_status(name="my_pipe", level=AlertLevel.CRITICAL), config
    )
    assert "monitored" in result.tags
    assert "critical" in result.tags


def test_tag_pipeline_no_duplicate_tags():
    config = TaggerConfig(
        rules=[
            TagRule(tag="dup", level="OK"),
            TagRule(tag="dup", max_error_rate=0.5),
        ]
    )
    result = tag_pipeline(_make_status(level=AlertLevel.OK), config)
    assert result.tags.count("dup") == 1


def test_tag_pipeline_fields():
    config = TaggerConfig(rules=[TagRule(tag="t", level="WARNING")])
    status = _make_status(name="p1", level=AlertLevel.WARNING, error_rate=0.03)
    result = tag_pipeline(status, config)
    assert result.name == "p1"
    assert result.level == "WARNING"
    assert pytest.approx(result.error_rate, abs=1e-4) == 0.03


# ---------------------------------------------------------------------------
# tag_all
# ---------------------------------------------------------------------------

def test_tag_all_returns_one_per_status():
    config = TaggerConfig(rules=[])
    statuses = [_make_status(name=f"p{i}") for i in range(4)]
    results = tag_all(statuses, config)
    assert len(results) == 4


def test_tag_all_empty_input():
    config = TaggerConfig(rules=[TagRule(tag="x", level="OK")])
    assert tag_all([], config) == []
