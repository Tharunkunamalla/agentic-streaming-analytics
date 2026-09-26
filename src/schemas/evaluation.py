"""Data models for streaming, detection, agent, and adaptation evaluation metrics."""

from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field


class DetectionEvaluationMetrics(BaseModel):
    """Standard evaluation metrics for anomaly detection accuracy."""

    model_config = ConfigDict(frozen=True)

    detector_name: str
    precision: float = Field(ge=0.0, le=1.0)
    recall: float = Field(ge=0.0, le=1.0)
    f1_score: float = Field(ge=0.0, le=1.0)
    false_positive_rate: float = Field(ge=0.0, le=1.0)
    total_samples: int = Field(ge=0)
    true_positives: int = Field(ge=0)
    false_positives: int = Field(ge=0)
    true_negatives: int = Field(ge=0)
    false_negatives: int = Field(ge=0)


class StreamingEvaluationMetrics(BaseModel):
    """System and streaming performance evaluation metrics."""

    model_config = ConfigDict(frozen=True)

    throughput_events_per_sec: float = Field(ge=0.0)
    mean_detection_latency_ms: float = Field(ge=0.0)
    p95_detection_latency_ms: float = Field(ge=0.0)
    mean_processing_latency_ms: float = Field(ge=0.0)
    memory_usage_mb: float = Field(ge=0.0)
    window_count: int = Field(ge=0)


class AgentEvaluationMetrics(BaseModel):
    """Metrics evaluating the autonomous agentic orchestration layer."""

    model_config = ConfigDict(frozen=True)

    total_events_processed: int = Field(ge=0)
    decision_success_rate: float = Field(ge=0.0, le=1.0)
    unnecessary_tool_calls_rate: float = Field(ge=0.0, le=1.0)
    mean_decision_latency_ms: float = Field(ge=0.0)
    total_tokens_used: int = Field(ge=0)
    total_cost_usd: float = Field(ge=0.0)


class AdaptationEvaluationMetrics(BaseModel):
    """Metrics evaluating drift adaptation and detector switching."""

    model_config = ConfigDict(frozen=True)

    drift_detection_f1: float = Field(ge=0.0, le=1.0)
    detector_switch_frequency: float = Field(ge=0.0)
    adaptation_recovery_time_sec: float = Field(ge=0.0)
