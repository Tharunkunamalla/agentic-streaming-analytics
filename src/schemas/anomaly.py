"""Data models for detected anomaly events emitted over Kafka anomaly-events topic."""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4
from pydantic import BaseModel, ConfigDict, Field


class AnomalyEvent(BaseModel):
    """Structured anomaly event payload published to Kafka anomaly-events topic.

    Exclusively emitted when first-stage streaming detectors (e.g. AADS) flag
    an anomaly candidate. Nominal streaming events NEVER trigger this payload.
    """

    model_config = ConfigDict(frozen=True)

    event_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique anomaly event identifier",
    )
    timestamp: float = Field(
        description="Unix timestamp when the anomalous metric event occurred",
    )
    dataset: str = Field(
        default="AIOPS_KPI",
        description="Name of the underlying cloud metric dataset/stream source",
    )
    features: Dict[str, Any] = Field(
        default_factory=dict,
        description="Exact original event attributes (metric_id, value, ground_truth)",
    )
    anomaly_score: float = Field(
        description="Continuous anomaly confidence score from 0.0 to 1.0",
    )
    detector: str = Field(
        default="AADS",
        description="Identifier of the streaming detector that flagged the event",
    )
    recent_window_summary: Dict[str, float] = Field(
        default_factory=dict,
        description="Context summary statistics (mean, std, min, max, z_score)",
    )
    anomaly_frequency: float = Field(
        default=0.0,
        description="Recent frequency rate of anomaly triggers in the sliding window",
    )
    recent_detector_metrics: Dict[str, Any] = Field(
        default_factory=dict,
        description="First-stage detector operational health metrics (throughput, latency, memory)",
    )

    # Convenience aliases for backwards compatibility
    @property
    def metric_id(self) -> str:
        return str(self.features.get("metric_id", "default"))

    @property
    def value(self) -> float:
        return float(self.features.get("value", 0.0))

    @property
    def detector_name(self) -> str:
        return self.detector
