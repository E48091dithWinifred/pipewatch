"""Tests for pipewatch.tagger_filter."""
import pytest
from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.tagger import TaggedPipeline
from pipewatch.tagger_filter import TagFilterConfig, TagFilterResult, filter_by_tags


def _make_tagged(name: str, level: str, tags: list) -> TaggedPipeline:
    status = PipelineStatus(
        pipeline_name=name,
        level=AlertLevel[level],
        message="ok",
        error_rate=0.0,
        latency_ms=100.0,
    )
    return TaggedPipeline(status=status, tags=tags)


@pytest.fixture
def sample_tagged():
    return [
        _make_tagged("alpha", "OK", ["stable", "critical-path"]),
        _make_tagged("beta", "WARNING", ["flaky"]),
        _make_tagged("gamma", "CRITICAL", ["critical-path", "flaky"]),
        _make_tagged("delta", "OK", []),
    ]


def test_filter_no_config_returns_all(sample_tagged):
    result = filter_by_tags(sample_tagged)
    assert result.matched_count == 4
    assert result.dropped_count == 0


def test_filter_require_any(sample_tagged):
    cfg = TagFilterConfig(require_any=["flaky"])
    result = filter_by_tags(sample_tagged, cfg)
    names = [t.status.pipeline_name for t in result.matched]
    assert "beta" in names
    assert "gamma" in names
    assert "alpha" not in names


def test_filter_require_all(sample_tagged):
    cfg = TagFilterConfig(require_all=["critical-path", "flaky"])
    result = filter_by_tags(sample_tagged, cfg)
    assert result.matched_count == 1
    assert result.matched[0].status.pipeline_name == "gamma"


def test_filter_exclude(sample_tagged):
    cfg = TagFilterConfig(exclude=["flaky"])
    result = filter_by_tags(sample_tagged, cfg)
    names = [t.status.pipeline_name for t in result.matched]
    assert "beta" not in names
    assert "gamma" not in names
    assert "alpha" in names


def test_filter_require_any_and_exclude(sample_tagged):
    cfg = TagFilterConfig(require_any=["critical-path"], exclude=["flaky"])
    result = filter_by_tags(sample_tagged, cfg)
    assert result.matched_count == 1
    assert result.matched[0].status.pipeline_name == "alpha"


def test_filter_empty_input():
    result = filter_by_tags([], TagFilterConfig(require_any=["stable"]))
    assert result.matched_count == 0
    assert result.dropped_count == 0


def test_summary_format(sample_tagged):
    cfg = TagFilterConfig(require_any=["flaky"])
    result = filter_by_tags(sample_tagged, cfg)
    s = result.summary()
    assert "matched=" in s
    assert "dropped=" in s


def test_dropped_plus_matched_equals_total(sample_tagged):
    cfg = TagFilterConfig(require_any=["stable"])
    result = filter_by_tags(sample_tagged, cfg)
    assert result.matched_count + result.dropped_count == len(sample_tagged)
