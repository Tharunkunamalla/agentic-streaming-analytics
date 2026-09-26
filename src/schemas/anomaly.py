"""Data models for detected anomaly events emitted over Kafka."""

from datetime import datetime
from uuid import uuid4
from pydantic import BaseModel, ConfigDict, Field


class AnomalyEvent(BaseModel):
    """Represents a streaming anomaly event emitted by the first-stage detector."""

    model_config = ConfigDict(frozen=True)

    event_id: str = Field(
        default_factory=lambda: str(uuid4()), description="Unique anomaly event identifier"
    )
    timestamp: datetime = Field(
        description="Timestamp when the anomalous data point occurred"
    )
    metric_id: str = Field(
        description="Stream/KPI identifier experiencing the anomaly"
    )
    value: float = Field(
        description="Observed anomalous metric value"
    )
    detector_name: str = Field(
        default="AADS", description="Name of the streaming detector triggering the event"
    )
    anomaly_score: float = Field(
        description="Continuous anomaly score assigned by the baseline detector"
    )
    threshold: float = Field(
        description="Decision threshold active when the event was flagged"
    )
    window_mean: float = Field(
        description="Mean of the sliding baseline window at trigger time"
    )
    window_std: float = Field(
        description="Standard deviation of the sliding baseline window at trigger time"
    )
    recent_window: list[float] = Field(
        default_factory=list, description="Recent metric values for agentic context analysis"
    )
    ground_truth_label: int | None = Field(
        default=None, description="Ground truth benchmark label if available"
    )
