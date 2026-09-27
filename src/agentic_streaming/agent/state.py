"""Typed Agent State representation for the LangGraph state machine."""

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import TypedDict

from src.schemas.anomaly import AnomalyEvent


class AgentAction(str, Enum):
    """Allowed closed action space for the Autonomous Streaming Agent."""

    NO_ACTION = "NO_ACTION"
    INVESTIGATE = "INVESTIGATE"
    CHECK_DRIFT = "CHECK_DRIFT"
    COMPARE_DETECTORS = "COMPARE_DETECTORS"
    RUN_ALTERNATIVE_DETECTOR = "RUN_ALTERNATIVE_DETECTOR"
    REQUEST_DEEP_ANALYSIS = "REQUEST_DEEP_ANALYSIS"


class AgentDecisionOutput(BaseModel):
    """Structured Pydantic model returned by the LLM Planner node."""

    reasoning: str = Field(description="Analytical explanation of why this action was chosen")
    action: AgentAction = Field(description="Action selected from the allowed action space")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence score in the decision")
    action_params: Dict[str, Any] = Field(default_factory=dict, description="Parameters for the selected tool/action")


class AgentState(TypedDict):
    """Explicit, strongly typed LangGraph state dictionary."""

    event: AnomalyEvent
    profile: Dict[str, Any]
    plan: List[str]
    action: Optional[str]
    action_params: Dict[str, Any]
    tool_result: Dict[str, Any]
    validation_report: Dict[str, Any]
    memory_id: Optional[str]
    is_complete: bool
    retry_count: int
    fallback_used: bool
    error_log: List[str]
