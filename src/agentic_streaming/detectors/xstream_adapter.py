"""Official StreamAD xStream Detector Adapter.

Wraps streamad.model.xStream_Detector.xStreamDetector to provide the
CommonStreamingDetector interface:
- fit_score(event)
- reset()
- name()
"""

from typing import Any, Optional
import mmh3
import numpy as np

# Compatibility patch for mmh3 5.x numpy integer seed type
_orig_mmh3_hash = mmh3.hash
def _safe_mmh3_hash(s, signed=False, seed=0):
    return _orig_mmh3_hash(s, signed=signed, seed=int(seed))
mmh3.hash = _safe_mmh3_hash

from streamad.model.xStream_Detector import xStreamDetector
from src.agentic_streaming.detectors.interface import CommonStreamingDetector


class XStreamAdapter(CommonStreamingDetector):
    """Adapter for official StreamAD xStream detector."""

    def __init__(
        self,
        n_components: int = 25,
        n_chains: int = 50,
        depth: int = 10,
        window_len: int = 100,
        threshold: float = 0.50,
    ) -> None:
        super().__init__(threshold=threshold)
        self.n_components = n_components
        self.n_chains = n_chains
        self.depth = depth
        self.window_len = window_len
        self._init_detector()

    def _init_detector(self) -> None:
        self.detector = xStreamDetector(
            n_components=self.n_components,
            n_chains=self.n_chains,
            depth=self.depth,
            window_len=self.window_len,
        )

    def fit_score(self, event: Any) -> float:
        """Process event using official StreamAD xStream model."""
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
        """Reset internal StreamAD xStream model."""
        self.total_processed = 0
        self._init_detector()

    def name(self) -> str:
        return "xStream"
