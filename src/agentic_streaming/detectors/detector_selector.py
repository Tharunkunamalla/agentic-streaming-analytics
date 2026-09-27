"""Deterministic Detector Adaptation Engine for Autonomous Streaming Analytics.

Ensures LLM requests analysis actions (CHECK_DRIFT, COMPARE_DETECTORS) while
empirical performance criteria decide detector selection deterministically.
"""

from dataclasses import dataclass
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from src.agentic_streaming.tools.registry import ToolRegistry

logger = logging.getLogger("agentic_streaming.detector_selection")


@dataclass
class AdaptationPolicyConfig:
    """Predefined rules for deterministic detector switching."""

    psi_threshold: float = 0.20
    min_agreement_ratio: float = 0.50
    consensus_score_diff_threshold: float = 0.15
    evaluation_window_size: int = 100
    allowed_detectors: Tuple[str, ...] = ("AADS", "HSTree", "xStream", "RRCF")


class AutonomousDetectorSelector:
    """Manages detector selection policy deterministically based on tool benchmark results."""

    def __init__(self, policy_config: Optional[AdaptationPolicyConfig] = None) -> None:
        self.config = policy_config or AdaptationPolicyConfig()
        self.active_detector: str = "AADS"
        self.registry = ToolRegistry()
        self.adaptation_history: List[Dict[str, Any]] = []

    def evaluate_adaptation(
        self,
        event_id: str,
        agent_action: str,
        metric_value: float,
        window_history: List[float],
        baseline_window: List[float],
    ) -> Dict[str, Any]:
        """Process agent action through deterministic evaluation policy.

        Workflow:
        1. LLM requests action (e.g. CHECK_DRIFT or COMPARE_DETECTORS).
        2. Execute tool securely through ToolRegistry.
        3. If drift is detected or COMPARE_DETECTORS requested: run multi-detector benchmark.
        4. Select best detector deterministically according to evaluation rule.
        5. Return structured adaptation decision.
        """
        previous_detector = self.active_detector
        tool_executed = None
        tool_result = {}
        drift_detected = False
        reason = "No change requested."
        selected_detector = self.active_detector

        if agent_action == "CHECK_DRIFT":
            tool_executed = "check_drift"
            res = self.registry.execute(
                "check_drift",
                {"current_window": window_history, "baseline_window": baseline_window},
            )
            tool_result = res.get("result", {})
            if res.get("success"):
                psi = tool_result.get("psi", 0.0)
                drift_detected = tool_result.get("drift_detected", False) or psi >= self.config.psi_threshold

            # If meaningful concept drift is identified, run detector comparison
            if drift_detected:
                agent_action = "COMPARE_DETECTORS"
                reason = f"Concept drift detected (PSI={tool_result.get('psi', 0.0):.4f}). Triggering multi-detector benchmark."

        if agent_action == "COMPARE_DETECTORS":
            tool_executed = "compare_detectors"
            comp_res = self.registry.execute(
                "compare_detectors",
                {"value": metric_value, "window_history": window_history},
            )
            tool_result = comp_res.get("result", {})
            if comp_res.get("success"):
                scores = tool_result.get("scores", {})
                consensus = tool_result.get("consensus_anomaly", False)
                agreement_ratio = tool_result.get("agreement_ratio", 0.0)

                # Deterministic selection policy:
                # 1. If consensus anomaly and HSTree or RRCF shows stronger separation, switch to ensemble.
                # 2. If high drift and xStream density score > AADS, switch to xStream.
                # 3. Default to AADS baseline if scores are comparable.
                best_detector, selection_reason = self._apply_deterministic_policy(
                    scores, consensus, agreement_ratio, drift_detected
                )
                selected_detector = best_detector
                reason = selection_reason

        # Update active detector strictly from allowed set
        if selected_detector in self.config.allowed_detectors:
            self.active_detector = selected_detector

        adaptation_record = {
            "event_id": event_id,
            "agent_action": agent_action,
            "tool_executed": tool_executed,
            "previous_detector": previous_detector,
            "selected_detector": self.active_detector,
            "detector_changed": previous_detector != self.active_detector,
            "drift_detected": drift_detected,
            "tool_result": tool_result,
            "reason": reason,
        }
        self.adaptation_history.append(adaptation_record)
        logger.info(
            "Adaptation Evaluation for event %s: action=%s -> selected_detector=%s (changed=%s)",
            event_id,
            agent_action,
            self.active_detector,
            previous_detector != self.active_detector,
        )
        return adaptation_record

    def _apply_deterministic_policy(
        self,
        scores: Dict[str, float],
        consensus_anomaly: bool,
        agreement_ratio: float,
        drift_detected: bool,
    ) -> Tuple[str, str]:
        """Deterministic policy rule determining the best detector."""
        aads_score = scores.get("AADS", 0.0)
        hstree_score = scores.get("HSTree", 0.0)
        xstream_score = scores.get("xStream", 0.0)
        rrcf_score = scores.get("RRCF", 0.0)

        # Rule 1: High concept drift with strong density anomaly -> xStream
        if drift_detected and xstream_score > (aads_score + self.config.consensus_score_diff_threshold):
            return "xStream", f"xStream selected due to high concept drift and superior density separation ({xstream_score:.4f} vs {aads_score:.4f})."

        # Rule 2: Multi-detector consensus anomaly with high ensemble score -> HSTree
        if consensus_anomaly and hstree_score > (aads_score + self.config.consensus_score_diff_threshold):
            return "HSTree", f"HSTree selected due to multi-detector consensus ({agreement_ratio:.2f}) and higher tree isolation score ({hstree_score:.4f})."

        # Rule 3: Extreme localized spike -> RRCF
        if rrcf_score > 0.85 and rrcf_score > (aads_score + 0.20):
            return "RRCF", f"RRCF selected due to high collusive displacement score ({rrcf_score:.4f})."

        # Rule 4: Default baseline
        return "AADS", f"AADS baseline retained as default detector (score={aads_score:.4f})."
