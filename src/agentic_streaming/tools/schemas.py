"""Pydantic Input and Output Schemas for all 8 Controlled Agent Tools."""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


# 1. calculate_statistics
class CalculateStatisticsInput(BaseModel):
    values: List[float] = Field(description="List of metric values to calculate summary statistics for")
    current_value: Optional[float] = Field(default=None, description="Optional target value for z-score calculation")


class CalculateStatisticsOutput(BaseModel):
    mean: float
    std: float
    min: float
    max: float
    median: float
    p95: float
    z_score: float
    sample_count: int


# 2. check_drift
class CheckDriftInput(BaseModel):
    current_window: List[float] = Field(description="Recent stream metric values")
    baseline_window: List[float] = Field(description="Historical baseline metric values")


class CheckDriftOutput(BaseModel):
    drift_detected: bool
    ks_statistic: float
    p_value: float
    psi: float
    drift_severity: str


# 3, 4, 5. run_xstream, run_hstree, run_rrcf
class SingleDetectorInput(BaseModel):
    value: float = Field(description="Observed metric scalar value")
    window_history: List[float] = Field(default_factory=list, description="Recent historical metric window")


class SingleDetectorOutput(BaseModel):
    detector_name: str
    anomaly_score: float
    is_anomaly: bool
    execution_time_ms: float


# 6. compare_detectors
class CompareDetectorsInput(BaseModel):
    value: float = Field(description="Target metric value to evaluate across all detectors")
    window_history: List[float] = Field(default_factory=list, description="Recent metric window")


class CompareDetectorsOutput(BaseModel):
    scores: Dict[str, float]
    consensus_anomaly: bool
    agreement_ratio: float
    execution_time_ms: float


# 7. retrieve_similar_events
class RetrieveSimilarEventsInput(BaseModel):
    metric_id: str = Field(description="Target metric/stream identifier")
    anomaly_score: float = Field(description="Score of the target anomaly event")
    limit: int = Field(default=3, description="Maximum number of historical records to retrieve")


class RetrieveSimilarEventsOutput(BaseModel):
    similar_events: List[Dict[str, Any]]
    count: int


# 8. store_decision
class StoreDecisionInput(BaseModel):
    event_id: str = Field(description="Associated anomaly event ID")
    timestamp: str = Field(description="ISO timestamp or string representation")
    action: str = Field(description="Action selected by agent")
    tool: str = Field(description="Tool invoked by agent")
    result: str = Field(description="Tool output or return summary")
    success: bool = Field(default=True, description="Whether execution succeeded")
    latency_ms: float = Field(default=0.0, description="Tool execution duration in ms")
    detector: str = Field(default="AADS", description="Detector that flagged event")
    event_type: str = Field(default="ANOMALY_CANDIDATE", description="Event classification")


class StoreDecisionOutput(BaseModel):
    memory_id: str
    saved: bool
