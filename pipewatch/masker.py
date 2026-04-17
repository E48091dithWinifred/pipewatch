"""masker.py — redact or mask sensitive fields in pipeline statuses."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.checker import PipelineStatus, AlertLevel


@dataclass
class MaskConfig:
    redact_name: bool = False
    name_placeholder: str = "<redacted>"
    mask_error_rate: bool = False
    mask_latency: bool = False
    allowed_names: List[str] = field(default_factory=list)


@dataclass
class MaskedStatus:
    name: str
    level: str
    message: Optional[str]
    error_rate: Optional[float]
    latency_ms: Optional[float]
    masked_fields: List[str] = field(default_factory=list)

    @property
    def is_healthy(self) -> bool:
        return self.level == AlertLevel.OK.value

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "level": self.level,
            "message": self.message,
            "error_rate": self.error_rate,
            "latency_ms": self.latency_ms,
            "masked_fields": self.masked_fields,
        }


def mask_status(status: PipelineStatus, config: MaskConfig) -> MaskedStatus:
    masked: List[str] = []

    name = status.pipeline_name
    if config.redact_name and (
        not config.allowed_names or name not in config.allowed_names
    ):
        name = config.name_placeholder
        masked.append("name")

    error_rate: Optional[float] = status.error_rate
    if config.mask_error_rate:
        error_rate = None
        masked.append("error_rate")

    latency: Optional[float] = status.latency_ms
    if config.mask_latency:
        latency = None
        masked.append("latency_ms")

    return MaskedStatus(
        name=name,
        level=status.level.value,
        message=status.message,
        error_rate=error_rate,
        latency_ms=latency,
        masked_fields=masked,
    )


def mask_all(statuses: List[PipelineStatus], config: MaskConfig) -> List[MaskedStatus]:
    return [mask_status(s, config) for s in statuses]
