"""Data models for streaming detector configurations and comparative outputs."""

from enum import Enum
from pydantic import BaseModel, ConfigDict, Field


class DetectorType(str, Enum):
    """Supported streaming anomaly detectors."""

    AADS = "AADS"
    XSTREAM = "xStream"
    HSTREE = "HSTree"
    RRCF = "RRCF"


class DetectorScore(BaseModel):
    """Output score and classification from an individual detector."""

    model_config = ConfigDict(frozen=True)

    detector_name: DetectorType
    score: float = Field(description="Normalized anomaly score between 0.0 and 1.0 (or raw)")
    threshold: float = Field(description="Active decision threshold for this detector")
    is_anomaly: bool = Field(description="Binary anomaly decision by this detector")
    execution_time_ms: float = Field(description="Execution latency in milliseconds")


class DetectorComparisonResult(BaseModel):
    """Consolidated result from running multiple StreamAD-compatible detectors."""

    model_config = ConfigDict(frozen=True)

    metric_id: str = Field(description="Metric under evaluation")
    event_id: str = Field(description="Associated anomaly event identifier")
    scores: dict[str, DetectorScore] = Field(
        default_factory=dict, description="Mapping of detector names to their respective score records"
    )
    consensus_is_anomaly: bool = Field(
        description="Majority vote or ensemble anomaly flag"
    )
    recommended_detector: DetectorType = Field(
        description="The detector judged most suitable for the current streaming regime"
    )
    reasoning: str = Field(
        description="Justification for detector recommendation"
    )
