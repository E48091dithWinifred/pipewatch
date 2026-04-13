"""Tests for pipewatch/summarizer.py"""
import pytest

from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.summarizer import (
    SummaryLine,
    summarize,
    format_summary,
    print_summary,
)


def _make_status(
    name: str = "pipe_a",
    level: AlertLevel = AlertLevel.OK,
    error_rate: float = 0.0,
    latency_ms: float = 100.0,
    message: str = "",
) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        error_rate=error_rate,
        latency_ms=latency_ms,
        message=message,
    )


def test_summarize_empty_returns_empty():
    assert summarize([]) == []


def test_summarize_returns_summary_line_instances():
    statuses = [_make_status()]
    result = summarize(statuses)
    assert len(result) == 1
    assert isinstance(result[0], SummaryLine)


def test_summarize_line_fields():
    s = _make_status(name="etl_main", level=AlertLevel.WARNING, error_rate=0.05, latency_ms=320.0, message="high err")
    lines = summarize([s])
    line = lines[0]
    assert line.name == "etl_main"
    assert line.level == "WARNING"
    assert line.error_rate == pytest.approx(0.05)
    assert line.latency_ms == pytest.approx(320.0)
    assert line.message == "high err"


def test_summary_line_str_contains_name():
    line = SummaryLine(name="my_pipe", level="OK", error_rate=0.01, latency_ms=50.0, message="all good")
    assert "my_pipe" in str(line)


def test_summary_line_str_contains_level():
    line = SummaryLine(name="x", level="CRITICAL", error_rate=0.2, latency_ms=999.0, message="down")
    assert "CRITICAL" in str(line)


def test_format_summary_contains_title():
    result = format_summary([], title="My Report")
    assert "My Report" in result


def test_format_summary_counts_levels():
    statuses = [
        _make_status(level=AlertLevel.OK),
        _make_status(name="b", level=AlertLevel.WARNING),
        _make_status(name="c", level=AlertLevel.CRITICAL),
    ]
    result = format_summary(statuses)
    assert "OK: 1" in result
    assert "Warning: 1" in result
    assert "Critical: 1" in result


def test_format_summary_empty_shows_no_pipelines():
    result = format_summary([])
    assert "(no pipelines)" in result


def test_format_summary_includes_pipeline_name():
    statuses = [_make_status(name="special_pipe")]
    result = format_summary(statuses)
    assert "special_pipe" in result


def test_print_summary_outputs_to_stdout(capsys):
    statuses = [_make_status(name="visible_pipe")]
    print_summary(statuses, title="Test Output")
    captured = capsys.readouterr()
    assert "visible_pipe" in captured.out
    assert "Test Output" in captured.out
