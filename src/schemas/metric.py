"""Data models for raw stream metric telemetry records emitted to Kafka."""

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4
from pydantic import BaseModel, ConfigDict, Field, field_validator


class MetricRecord(BaseModel):
    """Represents a single time-series metric point emitted by the Kafka replay producer."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    event_id: str = Field(
        default_factory=lambda: str(uuid4()), description="Unique metric event UUID"
    )
    timestamp: float | int | datetime = Field(
        description="Original dataset timestamp (epoch seconds or datetime)"
    )
    ingestion_timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO UTC timestamp when emitted to Kafka",
    )
    metric_id: str = Field(
        default="kpi_default", description="Identifier of the KPI or host metric stream"
    )
    value: float = Field(
        description="Observed numeric metric value"
    )
    ground_truth: int | None = Field(
        default=None,
        ge=0,
        le=1,
        description="Ground truth binary anomaly label (0 = Normal, 1 = Anomaly)",
    )
    dataset: str = Field(
        default="AIOPS_KPI", description="Dataset identifier name"
    )
    source: str = Field(
        default="StreamAD", description="Origin or stream producer identifier"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional contextual key-value pairs"
    )

    @field_validator("timestamp", mode="before")
    @classmethod
    def convert_timestamp(cls, v: Any) -> float:
        """Convert datetime objects or strings to float epoch seconds."""
        if isinstance(v, datetime):
            return v.timestamp()
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            try:
                return float(v)
            except ValueError:
                dt = datetime.fromisoformat(v)
                return dt.timestamp()
        return float(v)

    @property
    def ground_truth_label(self) -> int | None:
        """Alias for ground_truth for evaluation compatibility."""
        return self.ground_truth


class MetricBatch(BaseModel):
    """A collection of metric records sent in a single batch."""

    model_config = ConfigDict(frozen=True)

    records: list[MetricRecord] = Field(
        default_factory=list, description="List of metric data points"
    )
    batch_id: str = Field(
        default_factory=lambda: str(uuid4()), description="Batch tracking UUID"
    )
