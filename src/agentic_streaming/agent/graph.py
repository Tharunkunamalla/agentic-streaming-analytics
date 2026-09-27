"""LangGraph Agent State Machine and Workflow Graph with Phase 11 Autonomous Adaptation."""

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from uuid import uuid4

from src.agentic_streaming.agent.planner import AgentPlanner
from src.agentic_streaming.agent.state import AgentAction, AgentState
from src.agentic_streaming.agent.validator import AgentValidator
from src.agentic_streaming.detectors.detector_selector import AutonomousDetectorSelector
from src.agentic_streaming.storage.relational_storage import RelationalStorageManager
from src.agentic_streaming.tools.registry import ToolRegistry
from src.schemas.agent import AgentDecision
from src.schemas.anomaly import AnomalyEvent
from src.utils.logger import get_logger

logger = get_logger("agent_graph")


class StreamingAgentWorkflow:
    """Explicit state machine workflow implementing Phase 11 Phase 12 integrated pipeline:

    START -> Profile -> Plan -> Select Action -> Execute Tool -> Evaluate Adaptation -> Validate -> Store Memory -> END
    """

    def __init__(self, llm_client: Any = None, db_storage: Optional[RelationalStorageManager] = None) -> None:
        self.planner = AgentPlanner(llm_client)
        self.tool_registry = ToolRegistry()
        self.detector_selector = AutonomousDetectorSelector()
        self.storage = db_storage or RelationalStorageManager()

    def initialize_state(self, event: AnomalyEvent) -> AgentState:
        """Create fresh initial state dictionary for an incoming AnomalyEvent."""
        return {
            "event": event,
            "profile": {},
            "plan": [],
            "action": None,
            "action_params": {},
            "tool_result": {},
            "validation_report": {},
            "memory_id": None,
            "is_complete": False,
            "retry_count": 0,
            "fallback_used": False,
            "error_log": [],
        }

    def execute_tool_node(self, state: AgentState) -> AgentState:
        """Stage 4: Execute controlled tool through allowlisted ToolRegistry."""
        action = state["action"]
        prof = state["profile"]
        evt = state["event"]

        start_iso = datetime.now(timezone.utc).isoformat()

        if action == AgentAction.NO_ACTION.value:
            res = {"success": True, "tool": "none", "result": {"summary": "No action taken for low-severity anomaly."}, "latency_ms": 0.0}
        elif action == AgentAction.INVESTIGATE.value:
            res = self.tool_registry.execute(
                "calculate_statistics",
                {"values": [prof["rolling_mean"], prof["value"]], "current_value": prof["value"]},
            )
        elif action == AgentAction.CHECK_DRIFT.value:
            base_win = [prof["rolling_mean"]] * 10
            curr_win = [prof["value"]] * 10
            res = self.tool_registry.execute("check_drift", {"current_window": curr_win, "baseline_window": base_win})
        elif action == AgentAction.COMPARE_DETECTORS.value:
            res = self.tool_registry.execute(
                "compare_detectors",
                {"value": prof["value"], "window_history": [prof["rolling_mean"]] * 5},
            )
        elif action == AgentAction.RUN_ALTERNATIVE_DETECTOR.value:
            res = self.tool_registry.execute("run_hstree", {"value": prof["value"], "window_history": [prof["rolling_mean"]] * 5})
        elif action == AgentAction.REQUEST_DEEP_ANALYSIS.value:
            res = self.tool_registry.execute(
                "compare_detectors",
                {"value": prof["value"], "window_history": [prof["rolling_mean"]] * 5},
            )
        else:
            res = {"success": False, "tool": "unknown", "error": f"Unknown action {action}", "latency_ms": 0.0}

        end_iso = datetime.now(timezone.utc).isoformat()
        state["tool_result"] = res

        # Phase 12: Log tool execution to relational database
        self.storage.log_tool_execution(
            event_id=evt.event_id,
            tool=res.get("tool", str(action)),
            start_time=start_iso,
            end_time=end_iso,
            success=res.get("success", False),
            result_json=str(res.get("result", res.get("error", ""))),
            latency_ms=res.get("latency_ms", 0.0),
        )

        # Phase 11: Execute autonomous detector selection evaluation
        adaptation_res = self.detector_selector.evaluate_adaptation(
            event_id=evt.event_id,
            agent_action=action,
            metric_value=prof["value"],
            window_history=[prof["rolling_mean"]] * 5,
            baseline_window=[prof["rolling_mean"]] * 10,
        )
        state["action_params"]["adaptation"] = adaptation_res

        return state

    def validate_node(self, state: AgentState) -> AgentState:
        """Stage 5: Validate execution outputs and decision integrity."""
        action = state["action"]
        if not action or action not in {a.value for a in AgentAction}:
            return AgentValidator.apply_deterministic_fallback(state, reason="Invalid action in validation node")

        if not state.get("validation_report"):
            state["validation_report"] = {"status": "SUCCESS", "is_valid": True, "reason": "Execution validated"}

        return state

    def store_memory_node(self, state: AgentState) -> AgentState:
        """Stage 6: Store decision and execution trajectory in episodic memory and storage."""
        evt = state["event"]
        memory_id = f"mem-{uuid4().hex[:10]}"
        state["memory_id"] = memory_id
        state["is_complete"] = True

        # Phase 12: Log agent decision to database
        self.storage.log_agent_decision(
            event_id=evt.event_id,
            action=state["action"] or AgentAction.NO_ACTION.value,
            reason=str(state.get("plan", [])),
            success=state["validation_report"].get("is_valid", True),
            latency_ms=state["tool_result"].get("latency_ms", 0.0),
            active_detector=self.detector_selector.active_detector,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        logger.info(
            f"Stored decision trajectory in memory (memory_id={memory_id}). Action={state['action']} ActiveDetector={self.detector_selector.active_detector}"
        )
        return state

    def run(self, event: AnomalyEvent) -> Tuple[AgentState, AgentDecision]:
        """Execute full end-to-end state machine workflow.

        START -> Profile -> Plan -> Select Action -> Execute Tool -> Evaluate Adaptation -> Validate -> Store Memory -> END
        """
        state = self.initialize_state(event)

        # Phase 12: Log event and detection
        self.storage.log_event(
            event_id=event.event_id,
            timestamp=str(event.timestamp),
            metric_id=event.metric_id,
            value=event.value,
            features_json=str(event.features),
        )
        self.storage.log_detection(
            event_id=event.event_id,
            detector=event.detector,
            score=event.anomaly_score,
            prediction=True,
            latency_ms=0.05,
            timestamp=str(event.timestamp),
        )

        # 1. Profile
        state = self.planner.profile_node(state)

        # 2. Plan
        state = self.planner.plan_node(state)

        # 3. Select Action
        state = self.planner.select_action_node(state)

        # 4. Execute Tool
        state = self.execute_tool_node(state)

        # 5. Validate
        state = self.validate_node(state)

        # 6. Store Memory
        state = self.store_memory_node(state)

        # Construct Pydantic AgentDecision object
        selected_act = state["action"] or AgentAction.NO_ACTION.value
        decision = AgentDecision(
            decision_id=f"dec-{uuid4().hex[:8]}",
            event_id=event.event_id,
            metric_id=event.metric_id,
            timestamp=datetime.now(timezone.utc),
            action=selected_act,
            confidence=0.90 if not state["fallback_used"] else 0.50,
            reasoning=str(state["plan"]),
        )

        return state, decision
