"""Data models for LangGraph agent actions, decisions, and execution traces."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4
from pydantic import BaseModel, ConfigDict, Field

from src.schemas.detector import DetectorType


class AgentActionType(str, Enum):
    """Permitted predefined autonomous agent actions."""

    NO_ACTION = "NO_ACTION"
    INVESTIGATE = "INVESTIGATE"
    CHECK_DRIFT = "CHECK_DRIFT"
    COMPARE_DETECTORS = "COMPARE_DETECTORS"
    RUN_ALTERNATIVE_DETECTOR = "RUN_ALTERNATIVE_DETECTOR"
    REQUEST_DEEP_ANALYSIS = "REQUEST_DEEP_ANALYSIS"


class ToolCallRecord(BaseModel):
    """Record of an individual tool executed during agent reasoning."""

    model_config = ConfigDict(frozen=True)

    tool_name: str = Field(description="Name of the invoked tool")
    arguments: dict[str, Any] = Field(default_factory=dict, description="Input arguments provided to the tool")
    output_summary: str = Field(description="Summary of tool output or return payload")
    duration_ms: float = Field(default=0.0, description="Execution duration in milliseconds")
    success: bool = Field(default=True, description="Whether tool executed successfully")


class AgentDecision(BaseModel):
    """Final structured decision synthesized by the LangGraph agent."""

    model_config = ConfigDict(frozen=True)

    decision_id: str = Field(
        default_factory=lambda: str(uuid4()), description="Unique identifier for this decision"
    )
    event_id: str = Field(
        description="Associated anomaly event ID"
    )
    metric_id: str = Field(
        description="Target metric identifier"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="Timestamp of decision generation"
    )
    action: AgentActionType = Field(
        description="Primary action selected by the agent"
    )
    confidence: float = Field(
        ge=0.0, le=1.0, description="Agent confidence score in the chosen action"
    )
    reasoning: str = Field(
        description="Structured chain-of-thought or justification for the action"
    )
    selected_detector: DetectorType | None = Field(
        default=None, description="Detector recommended or switched to, if applicable"
    )
    tool_calls: list[ToolCallRecord] = Field(
        default_factory=list, description="Sequence of tool invocations performed"
    )
    drift_detected: bool | None = Field(
        default=None, description="Flag indicating if drift was verified"
    )
    latency_ms: float = Field(
        default=0.0, description="Total agentic reasoning latency in milliseconds"
    )
    tokens_used: int | None = Field(
        default=None, description="Total LLM tokens consumed during reasoning"
    )
    cost_usd: float | None = Field(
        default=None, description="Estimated LLM API cost in USD"
    )
