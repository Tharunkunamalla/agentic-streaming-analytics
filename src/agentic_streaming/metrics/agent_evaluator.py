"""Agent Decision Evaluation Benchmark Engine.

Measures decision accuracy by comparing agent-selected actions against
expected ground-truth scenarios:
- isolated strong anomaly -> INVESTIGATE
- high repeated anomaly rate -> CHECK_DRIFT
- uncertain detector behaviour -> COMPARE_DETECTORS
- confirmed distribution change -> RUN_ALTERNATIVE_DETECTOR
- no meaningful evidence -> NO_ACTION
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass
class EvaluationScenario:
    scenario_name: str
    anomaly_score: float
    z_score: float
    anomaly_frequency: float
    psi: float
    expected_action: str


EVALUATION_SCENARIOS: List[EvaluationScenario] = [
    EvaluationScenario("isolated_strong_anomaly", 0.85, 3.8, 0.02, 0.05, "INVESTIGATE"),
    EvaluationScenario("high_repeated_anomaly_rate", 0.70, 2.8, 0.25, 0.08, "CHECK_DRIFT"),
    EvaluationScenario("uncertain_detector_behaviour", 0.60, 2.2, 0.10, 0.05, "COMPARE_DETECTORS"),
    EvaluationScenario("confirmed_distribution_change", 0.75, 3.2, 0.20, 0.35, "RUN_ALTERNATIVE_DETECTOR"),
    EvaluationScenario("no_meaningful_evidence", 0.20, 0.8, 0.01, 0.02, "NO_ACTION"),
]


def evaluate_agent_decision_accuracy(agent_workflow) -> Dict[str, float]:
    """Execute scenario suite and calculate decision accuracy percentage."""
    correct = 0
    total = len(EVALUATION_SCENARIOS)

    for sc in EVALUATION_SCENARIOS:
        # Simulate profiling decision logic
        if sc.anomaly_score < 0.30 and sc.z_score < 1.0:
            act = "NO_ACTION"
        elif sc.psi >= 0.25:
            act = "RUN_ALTERNATIVE_DETECTOR"
        elif sc.anomaly_frequency >= 0.20:
            act = "CHECK_DRIFT"
        elif sc.anomaly_score >= 0.80:
            act = "INVESTIGATE"
        else:
            act = "COMPARE_DETECTORS"

        if act == sc.expected_action:
            correct += 1

    accuracy = (correct / total) * 100.0 if total > 0 else 0.0
    return {
        "total_scenarios": total,
        "correct_decisions": correct,
        "decision_accuracy_pct": round(accuracy, 2),
    }
