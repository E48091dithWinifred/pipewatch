"""Tests for pipewatch.tee."""
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.tee import TeeConfig, TeeResult, tee_statuses


def _make_status(name: str, level: AlertLevel = AlertLevel.OK) -> PipelineStatus:
    return PipelineStatus(pipeline_name=name, level=level, message="ok", error_rate=0.0, latency_ms=10.0)


@pytest.fixture
def sample_statuses():
    return [
        _make_status("pipe-a"),
        _make_status("pipe-b", AlertLevel.WARNING),
        _make_status("pipe-c", AlertLevel.CRITICAL),
    ]


def test_tee_config_defaults():
    cfg = TeeConfig()
    assert cfg.outputs == ["primary", "secondary"]


def test_tee_config_invalid_empty_raises():
    with pytest.raises(ValueError, match="at least one"):
        TeeConfig(outputs=[])


def test_tee_config_duplicate_names_raises():
    with pytest.raises(ValueError, match="unique"):
        TeeConfig(outputs=["a", "a"])


def test_tee_result_output_names(sample_statuses):
    result = tee_statuses(sample_statuses, TeeConfig(outputs=["x", "y"]))
    assert result.output_names == ["x", "y"]


def test_tee_result_total_slots(sample_statuses):
    result = tee_statuses(sample_statuses, TeeConfig(outputs=["a", "b", "c"]))
    assert result.total_slots == 3


def test_tee_each_slot_has_all_statuses(sample_statuses):
    result = tee_statuses(sample_statuses)
    for name in result.output_names:
        assert len(result.get(name)) == len(sample_statuses)


def test_tee_slots_are_independent_copies(sample_statuses):
    result = tee_statuses(sample_statuses)
    primary = result.get("primary")
    secondary = result.get("secondary")
    assert primary is not secondary
    assert primary == secondary


def test_tee_empty_statuses_returns_empty_slots():
    result = tee_statuses([], TeeConfig(outputs=["only"]))
    assert result.get("only") == []


def test_tee_get_missing_name_returns_empty(sample_statuses):
    result = tee_statuses(sample_statuses)
    assert result.get("nonexistent") == []


def test_tee_summary_contains_slot_names(sample_statuses):
    result = tee_statuses(sample_statuses)
    s = result.summary()
    assert "primary" in s
    assert "secondary" in s


def test_tee_no_config_uses_defaults(sample_statuses):
    result = tee_statuses(sample_statuses)
    assert set(result.output_names) == {"primary", "secondary"}
