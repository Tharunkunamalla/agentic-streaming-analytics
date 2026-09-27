"""Official StreamAD HSTree Detector Adapter.

Wraps streamad.model.hstree_Detector.HSTreeDetector to provide the
CommonStreamingDetector interface:
- fit_score(event)
- reset()
- name()
"""

from typing import Any
import numpy as np
from streamad.model.hstree_Detector import HSTreeDetector
from src.agentic_streaming.detectors.interface import CommonStreamingDetector


class HSTreeAdapter(CommonStreamingDetector):
    """Adapter for official StreamAD HSTree detector."""

    def __init__(
        self,
        tree_height: int = 10,
        tree_num: int = 25,
        threshold: float = 0.50,
    ) -> None:
        super().__init__(threshold=threshold)
        self.tree_height = tree_height
        self.tree_num = tree_num
        self._init_detector()

    def _init_detector(self) -> None:
        self.detector = HSTreeDetector(
            tree_height=self.tree_height,
            tree_num=self.tree_num,
        )

    def fit_score(self, event: Any) -> float:
        """Process event using official StreamAD HSTree model."""
        if isinstance(event, dict):
            val = float(event["value"])
        elif hasattr(event, "value"):
            val = float(event.value)
        elif hasattr(event, "__len__") and len(event) == 1:
            val = float(event[0])
        else:
            val = float(event)

        x = np.array([val], dtype=float)
        score = self.detector.fit_score(x)
        if score is None:
            return 0.0
        return float(np.clip(score, 0.0, 1.0))

    def reset(self) -> None:
        """Reset internal StreamAD HSTree model."""
        self.total_processed = 0
        self._init_detector()

    def name(self) -> str:
        return "HSTree"
