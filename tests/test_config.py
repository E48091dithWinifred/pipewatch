"""Tests for pipewatch configuration loading."""

import os
import pytest
from pipewatch.config import load_config, PipewatchConfig, PipelineConfig, AlertThresholds

FIXTURE_CONFIG = os.path.join(os.path.dirname(__file__), "fixtures", "sample_config.yaml")


def test_load_config_returns_correct_type():
    config = load_config(FIXTURE_CONFIG)
    assert isinstance(config, PipewatchConfig)


def test_load_config_global_settings():
    config = load_config(FIXTURE_CONFIG)
    assert config.check_interval_seconds == 60
    assert config.log_level == "DEBUG"


def test_load_config_pipeline_count():
    config = load_config(FIXTURE_CONFIG)
    assert len(config.pipelines) == 3


def test_load_config_pipeline_fields():
    config = load_config(FIXTURE_CONFIG)
    pipeline = config.pipelines[0]
    assert isinstance(pipeline, PipelineConfig)
    assert pipeline.name == "orders_etl"
    assert pipeline.source == "postgres://localhost/warehouse"
    assert pipeline.enabled is True
    assert "finance" in pipeline.tags
    assert "critical" in pipeline.tags


def test_load_config_thresholds():
    config = load_config(FIXTURE_CONFIG)
    thresholds = config.pipelines[0].thresholds
    assert isinstance(thresholds, AlertThresholds)
    assert thresholds.max_duration_seconds == 120.0
    assert thresholds.max_error_rate == 0.01
    assert thresholds.min_rows_processed == 100
    assert thresholds.max_lag_seconds == 30.0


def test_load_config_disabled_pipeline():
    config = load_config(FIXTURE_CONFIG)
    legacy = next(p for p in config.pipelines if p.name == "legacy_import")
    assert legacy.enabled is False


def test_load_config_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/config.yaml")


def test_load_config_default_thresholds():
    config = load_config(FIXTURE_CONFIG)
    pipeline = config.pipelines[1]
    assert pipeline.thresholds.max_duration_seconds == 60.0
    assert pipeline.thresholds.max_error_rate == 0.05
