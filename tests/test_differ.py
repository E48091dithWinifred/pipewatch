"""Tests for pipewatch.differ."""
import pytest
from pipewatch.checker import AlertLevel, PipelineStatus
from pipewatch.differ import FieldDiff, DiffResult, diff_status, diff_all


def _make_status(name="pipe", level=AlertLevel.OK, message="ok",
                 error_rate=0.0, latency_ms=100.0) -> PipelineStatus:
    return PipelineStatus(
        pipeline=name,
        level=level,
        message=message,
        error_rate=error_rate,
        latency_ms=latency_ms,
    )


def test_field_diff_changed_when_values_differ():
    fd = FieldDiff(field="level", before=AlertLevel.OK, after=AlertLevel.WARNING)
    assert fd.changed is True


def test_field_diff_not_changed_when_equal():
    fd = FieldDiff(field="level", before=AlertLevel.OK, after=AlertLevel.OK)
    assert fd.changed is False


def test_field_diff_summary_contains_field():
    fd = FieldDiff(field="error_rate", before=0.0, after=0.5)
    assert "error_rate" in fd.summary()


def test_diff_status_no_changes():
    s = _make_status()
    result = diff_status(s, s)
    assert result.has_changes is False


def test_diff_status_level_change():
    before = _make_status(level=AlertLevel.OK)
    after = _make_status(level=AlertLevel.CRITICAL)
    result = diff_status(before, after)
    assert result.has_changes is True
    assert "level" in result.changed_fields


def test_diff_status_error_rate_change():
    before = _make_status(error_rate=0.0)
    after = _make_status(error_rate=0.9)
    result = diff_status(before, after)
    assert "error_rate" in result.changed_fields


def test_diff_status_pipeline_name_preserved():
    before = _make_status(name="alpha")
    after = _make_status(name="alpha")
    result = diff_status(before, after)
    assert result.pipeline == "alpha"


def test_diff_status_summary_no_changes():
    s = _make_status()
    result = diff_status(s, s)
    assert "no changes" in result.summary()


def test_diff_status_summary_shows_field():
    before = _make_status(latency_ms=100.0)
    after = _make_status(latency_ms=999.0)
    result = diff_status(before, after)
    assert "latency_ms" in result.summary()


def test_diff_all_returns_only_changed():
    before = [_make_status("a"), _make_status("b")]
    after = [_make_status("a"), _make_status("b", level=AlertLevel.WARNING)]
    results = diff_all(before, after)
    assert len(results) == 1
    assert results[0].pipeline == "b"


def test_diff_all_skips_new_pipelines():
    before = [_make_status("a")]
    after = [_make_status("a"), _make_status("new")]
    results = diff_all(before, after)
    names = [r.pipeline for r in results]
    assert "new" not in names


def test_diff_all_empty_returns_empty():
    assert diff_all([], []) == []
