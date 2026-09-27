"""Agent Profiling and Action Planning Node."""

import math
from typing import Any, Dict
from src.agentic_streaming.agent.prompts import SYSTEM_PROMPT
from src.agentic_streaming.agent.state import AgentAction, AgentDecisionOutput, AgentState
from src.agentic_streaming.agent.validator import AgentValidator
from src.utils.logger import get_logger

logger = get_logger("agent_planner")


class AgentPlanner:
    """Profiles anomaly event context and plans action selection."""

    def __init__(self, llm_client: Any = None) -> None:
        self.llm_client = llm_client

    def profile_node(self, state: AgentState) -> AgentState:
        """Stage 1: Profile anomaly event context."""
        evt = state["event"]
        summary = evt.recent_window_summary or {}
        mean_val = float(summary.get("rolling_mean", evt.value))
        std_val = float(summary.get("rolling_std", 1.0))
        std_val = max(std_val, 1e-4)

        z_score = abs(evt.value - mean_val) / std_val

        profile = {
            "event_id": evt.event_id,
            "metric_id": evt.metric_id,
            "value": evt.value,
            "timestamp": evt.timestamp,
            "dataset": evt.dataset,
            "anomaly_score": evt.anomaly_score,
            "detector": evt.detector,
            "rolling_mean": mean_val,
            "rolling_std": std_val,
            "z_score": round(z_score, 4),
            "anomaly_frequency": evt.anomaly_frequency,
        }
        state["profile"] = profile
        logger.info(f"Profiled Event {evt.event_id[:8]}... z_score={z_score:.2f}, score={evt.anomaly_score:.4f}")
        return state

    def plan_node(self, state: AgentState) -> AgentState:
        """Stage 2: Formulate explicit analytical reasoning plan."""
        prof = state["profile"]
        plan = [
            f"Step 1: Inspect anomaly score ({prof['anomaly_score']:.4f}) and Z-Score ({prof['z_score']:.2f}).",
            f"Step 2: Evaluate recent anomaly frequency ({prof['anomaly_frequency']:.2f}).",
            "Step 3: Select constrained action from allowed action space.",
        ]
        state["plan"] = plan
        return state

    def select_action_node(self, state: AgentState) -> AgentState:
        """Stage 3: Select action via LLM or deterministic rule engine."""
        prof = state["profile"]

        # If LLM client is available, invoke structured completion
        if self.llm_client is not None:
            prompt_text = SYSTEM_PROMPT.format(
                dataset=prof["dataset"],
                detector=prof["detector"],
                metric_id=prof["metric_id"],
                value=prof["value"],
                anomaly_score=prof["anomaly_score"],
                recent_window_summary=state["event"].recent_window_summary,
                anomaly_frequency=prof["anomaly_frequency"],
            )

            try:
                raw_response = self.llm_client.invoke(prompt_text)
                is_valid, parsed, msg = AgentValidator.validate_raw_response(raw_response)
                if is_valid and parsed:
                    state["action"] = parsed.action.value
                    state["action_params"] = parsed.action_params
                    state["validation_report"] = {"status": "SUCCESS", "is_valid": True, "reason": msg}
                    return state

                # Retry once if initial attempt failed
                if state["retry_count"] == 0:
                    state["retry_count"] += 1
                    raw_response_retry = self.llm_client.invoke(prompt_text + "\nIMPORTANT: Return VALID JSON strictly matching schema.")
                    is_valid_retry, parsed_retry, msg_retry = AgentValidator.validate_raw_response(raw_response_retry)
                    if is_valid_retry and parsed_retry:
                        state["action"] = parsed_retry.action.value
                        state["action_params"] = parsed_retry.action_params
                        state["validation_report"] = {"status": "SUCCESS_RETRY", "is_valid": True, "reason": msg_retry}
                        return state

                # If retry fails, apply deterministic fallback
                return AgentValidator.apply_deterministic_fallback(state, reason=f"LLM validation failed: {msg}")

            except Exception as e:
                return AgentValidator.apply_deterministic_fallback(state, reason=f"LLM invocation error: {e}")

        # Deterministic default rule selection when LLM is not connected
        score = float(prof["anomaly_score"])
        z_score = float(prof["z_score"])

        if score >= 0.85 or z_score >= 4.0:
            act = AgentAction.REQUEST_DEEP_ANALYSIS.value
        elif score >= 0.65 or z_score >= 3.0:
            act = AgentAction.COMPARE_DETECTORS.value
        elif score >= 0.45 or z_score >= 2.5:
            act = AgentAction.INVESTIGATE.value
        else:
            act = AgentAction.NO_ACTION.value

        state["action"] = act
        state["action_params"] = {"rule": "deterministic_rule_engine", "score": score, "z_score": z_score}
        state["validation_report"] = {"status": "SUCCESS", "is_valid": True, "reason": "Rule engine selection"}
        return state
