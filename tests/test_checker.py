"""Tests for the pipeline health checker module."""

import pytest

from pipewatch.checker import AlertLevel, PipelineStatus, check_pipeline
from pipewatch.config import AlertThresholds, PipelineConfig


@pytest.fixture
def sample_pipeline() -> PipelineConfig:
    thresholds = AlertThresholds(
        error_rate_warning=0.05,
        error_rate_critical=0.15,
        latency_warning_seconds=30.0,
        latency_critical_seconds=60.0,
        min_rows_processed=100,
    )
    return PipelineConfig(name="test_pipeline", source="db", destination="warehouse", thresholds=thresholds)


def test_check_pipeline_ok(sample_pipeline):
    status = check_pipeline(sample_pipeline, error_rate=0.01, latency_seconds=10.0, rows_processed=500)
    assert status.alert_level == AlertLevel.OK
    assert status.is_healthy is True
    assert status.pipeline_name == "test_pipeline"


def test_check_pipeline_warning_error_rate(sample_pipeline):
    status = check_pipeline(sample_pipeline, error_rate=0.08, latency_seconds=10.0, rows_processed=500)
    assert status.alert_level == AlertLevel.WARNING
    assert "error rate" in status.message
    assert status.is_healthy is False


def test_check_pipeline_critical_error_rate(sample_pipeline):
    status = check_pipeline(sample_pipeline, error_rate=0.20, latency_seconds=10.0, rows_processed=500)
    assert status.alert_level == AlertLevel.CRITICAL
    assert status.is_healthy is False


def test_check_pipeline_warning_latency(sample_pipeline):
    status = check_pipeline(sample_pipeline, error_rate=0.01, latency_seconds=45.0, rows_processed=500)
    assert status.alert_level == AlertLevel.WARNING
    assert "latency" in status.message


def test_check_pipeline_critical_latency(sample_pipeline):
    status = check_pipeline(sample_pipeline, error_rate=0.01, latency_seconds=75.0, rows_processed=500)
    assert status.alert_level == AlertLevel.CRITICAL


def test_check_pipeline_low_rows_processed(sample_pipeline):
    status = check_pipeline(sample_pipeline, error_rate=0.01, latency_seconds=10.0, rows_processed=50)
    assert status.alert_level == AlertLevel.WARNING
    assert "rows processed" in status.message


def test_check_pipeline_critical_overrides_warning(sample_pipeline):
    """Critical error rate should override a warning-level latency."""
    status = check_pipeline(sample_pipeline, error_rate=0.20, latency_seconds=45.0, rows_processed=500)
    assert status.alert_level == AlertLevel.CRITICAL


def test_pipeline_status_stores_metrics(sample_pipeline):
    status = check_pipeline(sample_pipeline, error_rate=0.03, latency_seconds=20.0, rows_processed=300)
    assert status.error_rate == 0.03
    assert status.latency_seconds == 20.0
    assert status.rows_processed == 300
