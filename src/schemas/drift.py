"""Data models for statistical drift analysis results."""

from datetime import datetime, timezone
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field


class DriftReport(BaseModel):
    """Encapsulates statistical drift detection findings for a metric stream."""

    model_config = ConfigDict(frozen=True)

    metric_id: str = Field(
        description="Identifier of the examined metric"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="Time drift inspection was executed"
    )
    is_drift_detected: bool = Field(
        description="Whether a concept/data drift was statistically confirmed"
    )
    method: Literal["ks_2sample", "page_hinkley", "adwin", "mann_whitney"] = Field(
        default="ks_2sample", description="Statistical drift testing algorithm"
    )
    statistic: float = Field(
        description="Calculated test statistic"
    )
    p_value: float | None = Field(
        default=None, description="P-value of the test (if applicable)"
    )
    change_point_index: int | None = Field(
        default=None, description="Estimated relative index where the drift shift began"
    )
    summary: str = Field(
        description="Human/Agent-readable interpretation of the drift test result"
    )
