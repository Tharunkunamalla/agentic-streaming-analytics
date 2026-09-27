"""Unit tests for Phase 8 LangGraph Agent Service, Validator, and State Machine."""

from unittest.mock import MagicMock
import pytest

from src.agentic_streaming.agent.graph import StreamingAgentWorkflow
from src.agentic_streaming.agent.planner import AgentPlanner
from src.agentic_streaming.agent.state import AgentAction, AgentDecisionOutput
from src.agentic_streaming.agent.validator import AgentValidator
from src.schemas.anomaly import AnomalyEvent


def test_agent_allowed_action_space():
    """Verify agent action space is strictly restricted to the 6 allowed actions."""
    allowed = {a.value for a in AgentAction}
    assert len(allowed) == 6
    assert "NO_ACTION" in allowed
    assert "INVESTIGATE" in allowed
    assert "CHECK_DRIFT" in allowed
    assert "COMPARE_DETECTORS" in allowed
    assert "RUN_ALTERNATIVE_DETECTOR" in allowed
    assert "REQUEST_DEEP_ANALYSIS" in allowed


def test_agent_validator_valid_json():
    """Verify AgentValidator validates compliant Pydantic decision outputs."""
    valid_json = """{
        "reasoning": "High anomaly score detected requiring investigation",
        "action": "INVESTIGATE",
        "confidence": 0.95,
        "action_params": {"metric_id": "kpi-123"}
    }"""
    is_valid, parsed, msg = AgentValidator.validate_raw_response(valid_json)
    assert is_valid is True
    assert parsed is not None
    assert parsed.action == AgentAction.INVESTIGATE
    assert parsed.confidence == 0.95


def test_agent_validator_invalid_action_trigger_fallback():
    """Verify invalid action selection is rejected and triggers deterministic fallback."""
    invalid_json = """{
        "reasoning": "Execute custom shell command",
        "action": "EXECUTE_SHELL",
        "confidence": 0.99
    }"""
    is_valid, parsed, msg = AgentValidator.validate_raw_response(invalid_json)
    assert is_valid is False
    assert parsed is None
    assert "validation error" in msg.lower() or "allowed action" in msg.lower()


def test_agent_deterministic_fallback():
    """Verify fallback mechanism produces valid deterministic actions based on severity."""
    event = AnomalyEvent(
        event_id="evt-fallback-1",
        timestamp=1700000000.0,
        dataset="AIOPS_KPI",
        features={"metric_id": "test-kpi", "value": 999.0},
        anomaly_score=0.92,
        detector="AADS",
    )
    workflow = StreamingAgentWorkflow()
    state = workflow.initialize_state(event)
    state = workflow.planner.profile_node(state)

    fallback_state = AgentValidator.apply_deterministic_fallback(state, reason="Invalid LLM JSON")

    assert fallback_state["fallback_used"] is True
    assert fallback_state["action"] == AgentAction.REQUEST_DEEP_ANALYSIS.value
    assert fallback_state["validation_report"]["status"] == "FALLBACK_APPLIED"


def test_streaming_agent_workflow_execution():
    """Verify full end-to-end workflow execution:

    START -> Profile -> Plan -> Select Action -> Execute Tool -> Validate -> Store Memory -> END
    """
    event = AnomalyEvent(
        event_id="evt-wf-100",
        timestamp=1700000500.0,
        dataset="AIOPS_KPI",
        features={"metric_id": "cpu-utilization", "value": 95.5},
        anomaly_score=0.88,
        detector="AADS",
        recent_window_summary={"rolling_mean": 25.0, "rolling_std": 2.5},
        anomaly_frequency=0.15,
    )

    workflow = StreamingAgentWorkflow()
    state, decision = workflow.run(event)

    # Validate state transitions
    assert state["is_complete"] is True
    assert state["memory_id"] is not None
    assert state["memory_id"].startswith("mem-")
    assert state["action"] in {a.value for a in AgentAction}

    # Validate tool execution
    assert state["tool_result"]["success"] is True
    assert "tool" in state["tool_result"]

    # Validate Pydantic AgentDecision payload
    assert decision.event_id == "evt-wf-100"
    assert decision.action.value == state["action"]
    assert decision.confidence > 0.0


def test_agent_planner_retry_on_invalid_llm():
    """Verify AgentPlanner retries once on invalid LLM output and falls back if still invalid."""
    mock_llm = MagicMock()
    # Return invalid JSON on first and second call
    mock_llm.invoke.side_effect = ["INVALID_NOT_JSON", "{\"action\": \"INVALID_ACT\"}"]

    event = AnomalyEvent(
        event_id="evt-retry-1",
        timestamp=1700000000.0,
        dataset="AIOPS_KPI",
        features={"value": 100.0},
        anomaly_score=0.70,
    )

    workflow = StreamingAgentWorkflow(llm_client=mock_llm)
    state, decision = workflow.run(event)

    assert mock_llm.invoke.call_count == 2
    assert state["fallback_used"] is True
    assert state["action"] == AgentAction.INVESTIGATE.value
