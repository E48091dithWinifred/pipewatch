"""Tests for pipewatch/masker.py"""
import pytest
from pipewatch.checker import PipelineStatus, AlertLevel
from pipewatch.masker import MaskConfig, MaskedStatus, mask_status, mask_all


def _make_status(name="pipe_a", level=AlertLevel.OK, error_rate=0.01, latency_ms=120.0):
    return PipelineStatus(
        pipeline_name=name,
        level=level,
        message=None,
        error_rate=error_rate,
        latency_ms=latency_ms,
    )


def test_mask_status_returns_masked_instance():
    s = _make_status()
    result = mask_status(s, MaskConfig())
    assert isinstance(result, MaskedStatus)


def test_mask_status_no_masking_preserves_all_fields():
    s = _make_status(name="pipe_a", error_rate=0.05, latency_ms=200.0)
    result = mask_status(s, MaskConfig())
    assert result.name == "pipe_a"
    assert result.error_rate == 0.05
    assert result.latency_ms == 200.0
    assert result.masked_fields == []


def test_mask_status_redacts_name():
    s = _make_status(name="secret_pipe")
    cfg = MaskConfig(redact_name=True)
    result = mask_status(s, cfg)
    assert result.name == "<redacted>"
    assert "name" in result.masked_fields


def test_mask_status_custom_placeholder():
    s = _make_status(name="secret_pipe")
    cfg = MaskConfig(redact_name=True, name_placeholder="***")
    result = mask_status(s, cfg)
    assert result.name == "***"


def test_mask_status_allowed_name_not_redacted():
    s = _make_status(name="allowed_pipe")
    cfg = MaskConfig(redact_name=True, allowed_names=["allowed_pipe"])
    result = mask_status(s, cfg)
    assert result.name == "allowed_pipe"
    assert "name" not in result.masked_fields


def test_mask_status_masks_error_rate():
    s = _make_status(error_rate=0.1)
    cfg = MaskConfig(mask_error_rate=True)
    result = mask_status(s, cfg)
    assert result.error_rate is None
    assert "error_rate" in result.masked_fields


def test_mask_status_masks_latency():
    s = _make_status(latency_ms=500.0)
    cfg = MaskConfig(mask_latency=True)
    result = mask_status(s, cfg)
    assert result.latency_ms is None
    assert "latency_ms" in result.masked_fields


def test_mask_status_level_preserved_as_string():
    s = _make_status(level=AlertLevel.CRITICAL)
    result = mask_status(s, MaskConfig())
    assert result.level == "critical"


def test_mask_status_is_healthy_ok():
    s = _make_status(level=AlertLevel.OK)
    result = mask_status(s, MaskConfig())
    assert result.is_healthy is True


def test_mask_status_is_healthy_false_for_warning():
    s = _make_status(level=AlertLevel.WARNING)
    result = mask_status(s, MaskConfig())
    assert result.is_healthy is False


def test_mask_all_returns_list():
    statuses = [_make_status(name=f"pipe_{i}") for i in range(3)]
    results = mask_all(statuses, MaskConfig())
    assert len(results) == 3
    assert all(isinstance(r, MaskedStatus) for r in results)


def test_mask_status_as_dict_keys():
    s = _make_status()
    result = mask_status(s, MaskConfig()).as_dict()
    assert set(result.keys()) == {"name", "level", "message", "error_rate", "latency_ms", "masked_fields"}
