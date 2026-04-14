"""Tests for pipewatch/pruner.py"""

import pytest
from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.pruner import PruneConfig, PruneResult, prune


def _make_status(
    name: str = "pipe",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.01,
    latency_ms: float = 100.0,
    message: str = "",
) -> PipelineStatus:
    return PipelineStatus(
        pipeline=name,
        level=level,
        error_rate=error_rate,
        latency_ms=latency_ms,
        message=message,
    )


@pytest.fixture
def sample_statuses():
    return [
        _make_status("pipe_a", AlertLevel.OK, 0.0),
        _make_status("pipe_b", AlertLevel.WARNING, 0.05),
        _make_status("pipe_c", AlertLevel.CRITICAL, 0.20),
        _make_status("pipe_d", AlertLevel.OK, 0.01),
    ]


def test_prune_result_summary():
    result = PruneResult(kept=[], pruned=[])
    assert "kept=0" in result.summary()
    assert "pruned=0" in result.summary()


def test_prune_empty_config_keeps_all(sample_statuses):
    config = PruneConfig()
    result = prune(sample_statuses, config)
    assert result.kept_count == 4
    assert result.pruned_count == 0


def test_prune_remove_ok_level(sample_statuses):
    config = PruneConfig(remove_levels=["ok"])
    result = prune(sample_statuses, config)
    assert result.pruned_count == 2
    assert all(s.level != AlertLevel.OK for s in result.kept)


def test_prune_remove_warning_level(sample_statuses):
    config = PruneConfig(remove_levels=["warning"])
    result = prune(sample_statuses, config)
    assert result.pruned_count == 1
    assert result.pruned[0].pipeline == "pipe_b"


def test_prune_max_ok_count(sample_statuses):
    config = PruneConfig(remove_levels=["ok"], max_ok_count=1)
    result = prune(sample_statuses, config)
    # Only 1 OK kept, 1 pruned (plus the second OK is pruned by remove_levels)
    ok_kept = [s for s in result.kept if s.level == AlertLevel.OK]
    assert len(ok_kept) <= 1


def test_prune_max_error_rate_below_threshold(sample_statuses):
    config = PruneConfig(max_error_rate=0.02)
    result = prune(sample_statuses, config)
    pruned_names = {s.pipeline for s in result.pruned}
    assert "pipe_a" in pruned_names  # error_rate=0.0 < 0.02
    assert "pipe_d" in pruned_names  # error_rate=0.01 < 0.02


def test_prune_name_prefix_exclude(sample_statuses):
    config = PruneConfig(name_prefix_exclude="pipe_a")
    result = prune(sample_statuses, config)
    assert result.pruned_count == 1
    assert result.pruned[0].pipeline == "pipe_a"


def test_prune_combined_rules(sample_statuses):
    config = PruneConfig(
        remove_levels=["ok"],
        name_prefix_exclude="pipe_c",
    )
    result = prune(sample_statuses, config)
    kept_names = {s.pipeline for s in result.kept}
    assert "pipe_b" in kept_names
    assert "pipe_a" not in kept_names
    assert "pipe_c" not in kept_names


def test_prune_returns_prune_result_instance(sample_statuses):
    config = PruneConfig()
    result = prune(sample_statuses, config)
    assert isinstance(result, PruneResult)
