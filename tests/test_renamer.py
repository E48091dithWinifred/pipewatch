"""Tests for pipewatch.renamer."""
import pytest
from pipewatch.checker import PipelineStatus, AlertLevel
from pipewatch.renamer import RenameConfig, RenameResult, rename_statuses


def _make_status(name: str, level: AlertLevel = AlertLevel.OK) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message="ok",
        error_rate=0.0,
        latency_ms=10.0,
        checked_at="2024-01-01T00:00:00",
    )


def test_rename_config_get_new_name_mapped():
    cfg = RenameConfig(mapping={"old": "new"})
    assert cfg.get_new_name("old") == "new"


def test_rename_config_get_new_name_unmapped():
    cfg = RenameConfig(mapping={"old": "new"})
    assert cfg.get_new_name("other") == "other"


def test_rename_statuses_returns_rename_result():
    statuses = [_make_status("alpha")]
    cfg = RenameConfig(mapping={"alpha": "beta"})
    result = rename_statuses(statuses, cfg)
    assert isinstance(result, RenameResult)


def test_rename_statuses_applies_mapping():
    statuses = [_make_status("alpha")]
    cfg = RenameConfig(mapping={"alpha": "beta"})
    result = rename_statuses(statuses, cfg)
    assert result.renamed[0].pipeline_name == "beta"


def test_rename_statuses_preserves_unmapped():
    statuses = [_make_status("gamma")]
    cfg = RenameConfig(mapping={"alpha": "beta"})
    result = rename_statuses(statuses, cfg)
    assert result.renamed[0].pipeline_name == "gamma"


def test_rename_statuses_preserves_level():
    statuses = [_make_status("alpha", AlertLevel.CRITICAL)]
    cfg = RenameConfig(mapping={"alpha": "beta"})
    result = rename_statuses(statuses, cfg)
    assert result.renamed[0].level == AlertLevel.CRITICAL


def test_rename_statuses_changed_count():
    statuses = [_make_status("alpha"), _make_status("gamma")]
    cfg = RenameConfig(mapping={"alpha": "beta"})
    result = rename_statuses(statuses, cfg)
    assert result.changed_count == 1


def test_rename_statuses_empty_input():
    result = rename_statuses([], RenameConfig(mapping={"a": "b"}))
    assert result.renamed == []
    assert result.changed_count == 0


def test_rename_result_summary_contains_count():
    statuses = [_make_status("alpha"), _make_status("beta")]
    cfg = RenameConfig(mapping={"alpha": "renamed_alpha"})
    result = rename_statuses(statuses, cfg)
    assert "1" in result.summary()
    assert "2" in result.summary()
