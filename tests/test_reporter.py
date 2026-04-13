"""Tests for pipewatch.reporter."""

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.metrics import PipelineMetrics
from pipewatch.reporter import format_status_line, print_report


def _make_status(name, level, messages=None, metrics=None):
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        messages=messages or [],
        metrics=metrics,
    )


def test_format_ok_no_color():
    status = _make_status("orders", AlertLevel.OK)
    line = format_status_line(status, use_color=False)
    assert "[OK]" in line
    assert "orders" in line
    assert "all checks passed" in line


def test_format_warning_includes_message():
    status = _make_status("orders", AlertLevel.WARNING, messages=["high error rate"])
    line = format_status_line(status, use_color=False)
    assert "[WARNING]" in line
    assert "high error rate" in line


def test_format_critical_no_color():
    status = _make_status("shipments", AlertLevel.CRITICAL, messages=["latency critical"])
    line = format_status_line(status, use_color=False)
    assert "[CRITICAL]" in line
    assert "shipments" in line


def test_format_with_color_contains_escape():
    status = _make_status("orders", AlertLevel.OK)
    line = format_status_line(status, use_color=True)
    assert "\033[" in line


def test_format_multiple_messages_all_included():
    """All messages should appear in the formatted line, not just the first."""
    messages = ["high error rate", "low throughput", "stale data"]
    status = _make_status("orders", AlertLevel.WARNING, messages=messages)
    line = format_status_line(status, use_color=False)
    for msg in messages:
        assert msg in line, f"Expected message '{msg}' to appear in formatted line"


def test_print_report_returns_issue_count(capsys):
    statuses = [
        _make_status("a", AlertLevel.OK),
        _make_status("b", AlertLevel.WARNING, messages=["slow"]),
        _make_status("c", AlertLevel.CRITICAL, messages=["down"]),
    ]
    issues = print_report(statuses, use_color=False)
    assert issues == 2
    captured = capsys.readouterr()
    assert "[OK]" in captured.out
    assert "[WARNING]" in captured.out
    assert "[CRITICAL]" in captured.out


def test_print_report_verbose_shows_metrics(capsys):
    m = PipelineMetrics(
        pipeline_name="orders",
        records_processed=500,
        records_failed=5,
        duration_seconds=1.0,
    )
    statuses = [_make_status("orders", AlertLevel.WARNING, messages=["warn"], metrics=m)]
    print_report(statuses, use_color=False, verbose=True)
    captured = capsys.readouterr()
    assert "error_rate" in captured.out
    assert "latency" in captured.out
