"""Data models for enriched streaming metrics produced by Spark Structured Streaming."""

from datetime import datetime, timezone
from uuid import uuid4
from pydantic import BaseModel, ConfigDict, Field


class ProcessedMetricRecord(BaseModel):
    """Enriched metric record emitted to Kafka processed-metrics topic."""

    model_config = ConfigDict(frozen=True)

    event_id: str = Field(
        default_factory=lambda: str(uuid4()), description="Original or generated metric UUID"
    )
    timestamp: float = Field(
        description="Original metric timestamp in epoch seconds"
    )
    processing_timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO UTC timestamp when Spark processed the record",
    )
    metric_id: str = Field(
        description="Identifier of the KPI / metric time series"
    )
    value: float = Field(
        description="Observed numeric metric value"
    )
    ground_truth: int | None = Field(
        default=None, description="Benchmark ground truth label (0 = Normal, 1 = Anomaly)"
    )
    window_start: str | None = Field(
        default=None, description="Start timestamp of the active sliding aggregation window"
    )
    window_end: str | None = Field(
        default=None, description="End timestamp of the active sliding aggregation window"
    )
    rolling_mean: float = Field(
        description="Rolling window mean"
    )
    rolling_std: float = Field(
        default=0.0, description="Rolling window standard deviation"
    )
    rolling_min: float = Field(
        description="Rolling window minimum"
    )
    rolling_max: float = Field(
        description="Rolling window maximum"
    )
    rolling_count: int = Field(
        ge=1, description="Number of observations in the rolling window"
    )
    source: str = Field(
        default="SparkStructuredStreaming", description="Processing engine identifier"
    )
