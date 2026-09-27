"""Official StreamAD RRCF Detector Adapter.

Wraps streamad.model.rrcf_Detector.RrcfDetector to provide the
CommonStreamingDetector interface:
- fit_score(event)
- reset()
- name()
"""

from typing import Any
import numpy as np
from streamad.model.rrcf_Detector import RrcfDetector
from src.agentic_streaming.detectors.interface import CommonStreamingDetector


class RRCFAdapter(CommonStreamingDetector):
    """Adapter for official StreamAD RRCF detector."""

    def __init__(
        self,
        num_trees: int = 20,
        tree_size: int = 50,
        threshold: float = 0.50,
    ) -> None:
        super().__init__(threshold=threshold)
        self.num_trees = num_trees
        self.tree_size = tree_size
        self._init_detector()

    def _init_detector(self) -> None:
        self.detector = RrcfDetector(
            num_trees=self.num_trees,
            tree_size=self.tree_size,
        )

    def fit_score(self, event: Any) -> float:
        """Process event using official StreamAD RRCF model."""
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
        # In streamad RRCF, score can be large CoDisp, normalize with sigmoid or clipping
        norm_score = float(score) / (1.0 + float(score)) if score > 0 else 0.0
        return float(np.clip(norm_score, 0.0, 1.0))

    def reset(self) -> None:
        """Reset internal StreamAD RRCF model."""
        self.total_processed = 0
        self._init_detector()

    def name(self) -> str:
        return "RRCF"
