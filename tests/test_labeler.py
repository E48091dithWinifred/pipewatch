"""tests/test_labeler.py — Tests for pipewatch/labeler.py."""

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.labeler import (
    LabeledStatus,
    filter_by_tag,
    label_all,
    label_status,
)


def _make_status(
    name: str = "pipe",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float | None = None,
    latency_ms: float | None = None,
    message: str = "",
) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        error_rate=error_rate,
        latency_ms=latency_ms,
        message=message,
    )


def test_label_status_ok_badge():
    ls = label_status(_make_status(level=AlertLevel.OK))
    assert ls.badge == "✅"


def test_label_status_warning_badge():
    ls = label_status(_make_status(level=AlertLevel.WARNING))
    assert ls.badge == "⚠️"


def test_label_status_critical_badge():
    ls = label_status(_make_status(level=AlertLevel.CRITICAL))
    assert ls.badge == "🔴"


def test_label_status_healthy_tag():
    ls = label_status(_make_status(level=AlertLevel.OK))
    assert "healthy" in ls.tags
    assert "critical" not in ls.tags


def test_label_status_critical_tag():
    ls = label_status(_make_status(level=AlertLevel.CRITICAL))
    assert "critical" in ls.tags
    assert "healthy" not in ls.tags


def test_label_status_high_error_rate_tag():
    ls = label_status(_make_status(error_rate=0.10))
    assert "high-error-rate" in ls.tags


def test_label_status_no_high_error_rate_tag_below_threshold():
    ls = label_status(_make_status(error_rate=0.01))
    assert "high-error-rate" not in ls.tags


def test_label_status_slow_tag():
    ls = label_status(_make_status(latency_ms=1500.0))
    assert "slow" in ls.tags


def test_label_status_summary_no_issues():
    ls = label_status(_make_status())
    assert ls.summary == "no issues"


def test_label_status_summary_includes_error_rate():
    ls = label_status(_make_status(error_rate=0.05))
    assert "error_rate=" in ls.summary


def test_label_status_summary_includes_latency():
    ls = label_status(_make_status(latency_ms=800.0))
    assert "latency=" in ls.summary


def test_label_status_summary_includes_message():
    ls = label_status(_make_status(message="threshold exceeded"))
    assert "threshold exceeded" in ls.summary


def test_label_all_returns_same_count():
    statuses = [_make_status(name=f"p{i}") for i in range(4)]
    result = label_all(statuses)
    assert len(result) == 4


def test_label_all_preserves_names():
    statuses = [_make_status(name="alpha"), _make_status(name="beta")]
    names = [ls.pipeline_name for ls in label_all(statuses)]
    assert names == ["alpha", "beta"]


def test_filter_by_tag_returns_matching():
    labeled = [
        label_status(_make_status(name="a", level=AlertLevel.CRITICAL)),
        label_status(_make_status(name="b", level=AlertLevel.OK)),
    ]
    result = filter_by_tag(labeled, "critical")
    assert len(result) == 1
    assert result[0].pipeline_name == "a"


def test_filter_by_tag_empty_when_none_match():
    labeled = [label_status(_make_status(level=AlertLevel.OK))]
    assert filter_by_tag(labeled, "slow") == []
