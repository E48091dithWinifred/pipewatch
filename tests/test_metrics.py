"""Tests for pipewatch.metrics."""

import pytest

from pipewatch.metrics import MetricsSummary, PipelineMetrics


@pytest.fixture
def basic_metrics():
    return PipelineMetrics(
        pipeline_name="orders",
        records_processed=900,
        records_failed=100,
        duration_seconds=2.0,
    )


def test_error_rate(basic_metrics):
    assert basic_metrics.error_rate == pytest.approx(0.1)


def test_error_rate_zero_records():
    m = PipelineMetrics(pipeline_name="empty", records_processed=0, records_failed=0)
    assert m.error_rate == 0.0


def test_latency_ms(basic_metrics):
    assert basic_metrics.latency_ms == pytest.approx(2000.0)


def test_rows_per_second_auto_calculated(basic_metrics):
    assert basic_metrics.rows_per_second == pytest.approx(450.0)


def test_rows_per_second_zero_duration():
    m = PipelineMetrics(
        pipeline_name="fast", records_processed=100, duration_seconds=0.0
    )
    assert m.rows_per_second is None


def test_summary_add_and_avg():
    summary = MetricsSummary(pipeline_name="orders")
    summary.add(PipelineMetrics("orders", records_processed=100, records_failed=10, duration_seconds=1.0))
    summary.add(PipelineMetrics("orders", records_processed=200, records_failed=0, duration_seconds=3.0))

    assert summary.total_records_processed == 300
    assert summary.total_records_failed == 10
    # avg error rate: (10/110 + 0/200) / 2
    expected_avg_error = (10 / 110 + 0.0) / 2
    assert summary.avg_error_rate == pytest.approx(expected_avg_error)
    assert summary.avg_latency_ms == pytest.approx(2000.0)  # (1000 + 3000) / 2


def test_summary_wrong_pipeline_raises():
    summary = MetricsSummary(pipeline_name="orders")
    with pytest.raises(ValueError, match="does not match"):
        summary.add(PipelineMetrics("shipments", records_processed=50))


def test_summary_empty_returns_zero():
    summary = MetricsSummary(pipeline_name="orders")
    assert summary.avg_error_rate == 0.0
    assert summary.avg_latency_ms == 0.0
