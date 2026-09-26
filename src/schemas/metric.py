"""Data models for raw stream metric telemetry records."""

from datetime import datetime
from typing import Any
from pydantic import BaseModel, ConfigDict, Field


class MetricRecord(BaseModel):
    """Represents a single time-series metric point emitted by the producer."""

    model_config = ConfigDict(frozen=True)

    timestamp: datetime = Field(
        description="Timestamp of the metric observation"
    )
    metric_id: str = Field(
        default="kpi_default", description="Identifier of the KPI or host metric stream"
    )
    value: float = Field(
        description="Observed numeric metric value"
    )
    ground_truth_label: int | None = Field(
        default=None, ge=0, le=1, description="Ground truth binary anomaly label (if available for benchmarking)"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional contextual key-value pairs"
    )


class MetricBatch(BaseModel):
    """A collection of metric records sent in a single batch."""

    records: list[MetricRecord] = Field(
        default_factory=list, description="List of metric data points"
    )
    batch_id: str | None = Field(
        default=None, description="Optional batch tracking UUID"
    )
