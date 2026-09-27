from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


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
    recent_window: List[float] = Field(
        default_factory=list,
        description="Recent metric values for agentic context analysis",
    )

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> float:
        if isinstance(v, datetime):
            return float(v.timestamp())
        return float(v)

    @model_validator(mode="before")
    @classmethod
    def pre_root_populate(cls, data: Any) -> Any:
        if isinstance(data, dict):
            feat = dict(data.get("features", {}))
            if "metric_id" in data and "metric_id" not in feat:
                feat["metric_id"] = data.pop("metric_id")
            if "value" in data and "value" not in feat:
                feat["value"] = data.pop("value")
            if "ground_truth" in data and "ground_truth" not in feat:
                feat["ground_truth"] = data.pop("ground_truth")
            data["features"] = feat

            summary = dict(data.get("recent_window_summary", {}))
            if "window_mean" in data and "rolling_mean" not in summary:
                summary["rolling_mean"] = data.pop("window_mean")
            if "window_std" in data and "rolling_std" not in summary:
                summary["rolling_std"] = data.pop("window_std")
            data["recent_window_summary"] = summary

            if "detector_name" in data and "detector" not in data:
                data["detector"] = data.pop("detector_name")
            else:
                data.pop("detector_name", None)

            data.pop("threshold", None)
            data.pop("ground_truth_label", None)
        return data

    # Convenience properties for backwards compatibility
    @property
    def metric_id(self) -> str:
        return str(self.features.get("metric_id", "default"))

    @property
    def value(self) -> float:
        return float(self.features.get("value", 0.0))

    @property
    def detector_name(self) -> str:
        return self.detector

    @property
    def threshold(self) -> float:
        return 0.50

    @property
    def window_mean(self) -> float:
        return float(self.recent_window_summary.get("rolling_mean", 0.0))

    @property
    def window_std(self) -> float:
        return float(self.recent_window_summary.get("rolling_std", 1.0))
