"""Configuration loading and validation for pipewatch."""

import os
import yaml
from dataclasses import dataclass, field
from typing import Optional


DEFAULT_CONFIG_PATH = os.path.expanduser("~/.pipewatch/config.yaml")


@dataclass
class AlertThresholds:
    max_duration_seconds: float = 300.0
    max_error_rate: float = 0.05
    min_rows_processed: int = 0
    max_lag_seconds: float = 60.0


@dataclass
class PipelineConfig:
    name: str
    source: str
    thresholds: AlertThresholds = field(default_factory=AlertThresholds)
    enabled: bool = True
    tags: list = field(default_factory=list)


@dataclass
class PipewatchConfig:
    pipelines: list[PipelineConfig] = field(default_factory=list)
    check_interval_seconds: int = 30
    log_level: str = "INFO"


def load_config(path: Optional[str] = None) -> PipewatchConfig:
    """Load and parse the pipewatch YAML configuration file."""
    config_path = path or DEFAULT_CONFIG_PATH

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError("Config file must be a YAML mapping.")

    pipelines = []
    for p in raw.get("pipelines", []):
        thresholds_raw = p.get("thresholds", {})
        thresholds = AlertThresholds(
            max_duration_seconds=thresholds_raw.get("max_duration_seconds", 300.0),
            max_error_rate=thresholds_raw.get("max_error_rate", 0.05),
            min_rows_processed=thresholds_raw.get("min_rows_processed", 0),
            max_lag_seconds=thresholds_raw.get("max_lag_seconds", 60.0),
        )
        pipelines.append(PipelineConfig(
            name=p["name"],
            source=p["source"],
            thresholds=thresholds,
            enabled=p.get("enabled", True),
            tags=p.get("tags", []),
        ))

    return PipewatchConfig(
        pipelines=pipelines,
        check_interval_seconds=raw.get("check_interval_seconds", 30),
        log_level=raw.get("log_level", "INFO"),
    )
