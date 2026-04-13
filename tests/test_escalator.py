"""Tests for pipewatch.escalator."""
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.escalator import (
    EscalationConfig,
    EscalationRule,
    EscalationState,
    escalate,
)


def _make_status(level: AlertLevel, name: str = "pipe") -> PipelineStatus:
    return PipelineStatus(
        pipeline=name,
        level=level,
        message="msg",
        error_rate=0.05,
        latency_ms=100.0,
    )


@pytest.fixture
def config() -> EscalationConfig:
    return EscalationConfig(
        rules=[
            EscalationRule(from_level="WARNING", to_level="CRITICAL", after_runs=3)
        ]
    )


@pytest.fixture
def state() -> EscalationState:
    return EscalationState()


def test_ok_status_not_tracked(config, state):
    status = _make_status(AlertLevel.OK)
    result = escalate(status, config, state)
    assert result.level == AlertLevel.OK
    assert state.get("pipe") == 0


def test_warning_below_threshold_not_escalated(config, state):
    status = _make_status(AlertLevel.WARNING)
    for _ in range(2):
        result = escalate(status, config, state)
    assert result.level == AlertLevel.WARNING


def test_warning_at_threshold_escalated(config, state):
    status = _make_status(AlertLevel.WARNING)
    for _ in range(3):
        result = escalate(status, config, state)
    assert result.level == AlertLevel.CRITICAL


def test_escalated_message_contains_run_count(config, state):
    status = _make_status(AlertLevel.WARNING)
    for _ in range(3):
        result = escalate(status, config, state)
    assert "3" in result.message
    assert "escalated" in result.message


def test_critical_no_rule_resets_state(config, state):
    # Pre-populate a count then send OK to reset
    warn = _make_status(AlertLevel.WARNING)
    escalate(warn, config, state)
    escalate(warn, config, state)
    assert state.get("pipe") == 2

    ok = _make_status(AlertLevel.OK)
    escalate(ok, config, state)
    assert state.get("pipe") == 0


def test_different_pipelines_tracked_independently(config, state):
    w1 = _make_status(AlertLevel.WARNING, name="pipe_a")
    w2 = _make_status(AlertLevel.WARNING, name="pipe_b")
    for _ in range(3):
        escalate(w1, config, state)
    for _ in range(1):
        escalate(w2, config, state)
    assert state.get("pipe_a") == 3
    assert state.get("pipe_b") == 1


def test_no_rules_returns_status_unchanged(state):
    cfg = EscalationConfig(rules=[])
    status = _make_status(AlertLevel.WARNING)
    result = escalate(status, cfg, state)
    assert result.level == AlertLevel.WARNING


def test_escalation_preserves_error_rate_and_latency(config, state):
    status = _make_status(AlertLevel.WARNING)
    status = PipelineStatus(
        pipeline="pipe", level=AlertLevel.WARNING,
        message="bad", error_rate=0.12, latency_ms=250.0
    )
    for _ in range(3):
        result = escalate(status, config, state)
    assert result.error_rate == pytest.approx(0.12)
    assert result.latency_ms == pytest.approx(250.0)
