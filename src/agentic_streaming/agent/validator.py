"""Validation and Fallback Node for the LangGraph Agent Core."""

import json
from typing import Any, Dict, Optional, Tuple
from src.agentic_streaming.agent.state import AgentAction, AgentDecisionOutput, AgentState
from src.utils.logger import get_logger

logger = get_logger("agent_validator")

ALLOWED_ACTIONS = {a.value for a in AgentAction}


class AgentValidator:
    """Validates LLM action outputs and enforces deterministic safety fallbacks."""

    @staticmethod
    def validate_raw_response(raw_output: Any) -> Tuple[bool, Optional[AgentDecisionOutput], str]:
        """Validate raw LLM string or dict against Pydantic AgentDecisionOutput schema."""
        try:
            if isinstance(raw_output, str):
                # Clean code blocks if present
                clean_str = raw_output.strip()
                if clean_str.startswith("```"):
                    lines = clean_str.splitlines()
                    clean_str = "\n".join([l for l in lines if not l.startswith("```")])
                data = json.loads(clean_str)
            elif isinstance(raw_output, dict):
                data = raw_output
            else:
                return False, None, f"Unsupported response format: {type(raw_output)}"

            parsed = AgentDecisionOutput.model_validate(data)
            if parsed.action.value not in ALLOWED_ACTIONS:
                return False, None, f"Action '{parsed.action}' is not in allowed action space: {ALLOWED_ACTIONS}"

            return True, parsed, "Validation passed"

        except Exception as e:
            return False, None, f"Pydantic schema validation error: {e}"

    @classmethod
    def apply_deterministic_fallback(cls, state: AgentState, reason: str) -> AgentState:
        """Apply deterministic rule-based fallback decision when LLM output is invalid."""
        score = float(state["event"].anomaly_score or 0.0)

        if score >= 0.85:
            fallback_action = AgentAction.REQUEST_DEEP_ANALYSIS.value
        elif score >= 0.60:
            fallback_action = AgentAction.INVESTIGATE.value
        elif score >= 0.35:
            fallback_action = AgentAction.CHECK_DRIFT.value
        else:
            fallback_action = AgentAction.NO_ACTION.value

        state["action"] = fallback_action
        state["action_params"] = {"reasoning_source": "deterministic_fallback", "trigger_score": score}
        state["fallback_used"] = True
        state["validation_report"] = {
            "status": "FALLBACK_APPLIED",
            "is_valid": False,
            "fallback_action": fallback_action,
            "reason": reason,
        }
        logger.warning(
            f"LLM output validation failed ({reason}). Applied deterministic fallback action: {fallback_action}"
        )
        state["error_log"].append(f"Fallback applied: {reason}")
        return state
