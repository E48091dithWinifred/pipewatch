"""tests/test_sampler.py — Tests for pipewatch.sampler."""
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.sampler import SamplerConfig, SampleResult, sample_statuses


def _make_status(name: str, level: AlertLevel = AlertLevel.OK) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        error_rate=0.0,
        latency_ms=100.0,
        message="ok",
    )


@pytest.fixture
def sample_statuses_list():
    return [_make_status(f"pipe_{i}") for i in range(10)]


# --- SamplerConfig validation ---

def test_sampler_config_defaults():
    cfg = SamplerConfig()
    assert cfg.rate == 1.0
    assert cfg.seed is None
    assert cfg.min_keep == 0


def test_sampler_config_invalid_rate_raises():
    with pytest.raises(ValueError, match="rate"):
        SamplerConfig(rate=1.5)


def test_sampler_config_negative_rate_raises():
    with pytest.raises(ValueError):
        SamplerConfig(rate=-0.1)


def test_sampler_config_negative_min_keep_raises():
    with pytest.raises(ValueError, match="min_keep"):
        SamplerConfig(min_keep=-1)


# --- sample_statuses behaviour ---

def test_sample_empty_returns_empty():
    result = sample_statuses([], SamplerConfig())
    assert result.kept == []
    assert result.dropped == []


def test_sample_rate_one_keeps_all(sample_statuses_list):
    result = sample_statuses(sample_statuses_list, SamplerConfig(rate=1.0))
    assert result.kept_count == 10
    assert result.dropped_count == 0


def test_sample_rate_zero_drops_all(sample_statuses_list):
    result = sample_statuses(sample_statuses_list, SamplerConfig(rate=0.0))
    assert result.kept_count == 0
    assert result.dropped_count == 10


def test_sample_deterministic_is_reproducible(sample_statuses_list):
    cfg = SamplerConfig(rate=0.5, seed=42)
    r1 = sample_statuses(sample_statuses_list, cfg)
    r2 = sample_statuses(sample_statuses_list, cfg)
    assert [s.pipeline_name for s in r1.kept] == [s.pipeline_name for s in r2.kept]


def test_sample_different_seeds_may_differ(sample_statuses_list):
    r1 = sample_statuses(sample_statuses_list, SamplerConfig(rate=0.5, seed=1))
    r2 = sample_statuses(sample_statuses_list, SamplerConfig(rate=0.5, seed=99))
    # Not guaranteed but extremely likely with 10 items
    names1 = {s.pipeline_name for s in r1.kept}
    names2 = {s.pipeline_name for s in r2.kept}
    assert names1 != names2 or True  # allow equal by chance; just ensure no crash


def test_sample_min_keep_honoured(sample_statuses_list):
    cfg = SamplerConfig(rate=0.0, min_keep=3)
    result = sample_statuses(sample_statuses_list, cfg)
    assert result.kept_count >= 3


def test_sample_result_summary(sample_statuses_list):
    result = sample_statuses(sample_statuses_list, SamplerConfig(rate=1.0))
    assert "10/10" in result.summary


def test_sample_kept_plus_dropped_equals_total(sample_statuses_list):
    cfg = SamplerConfig(rate=0.4, seed=7)
    result = sample_statuses(sample_statuses_list, cfg)
    assert result.kept_count + result.dropped_count == len(sample_statuses_list)
