"""AADS Detector Adapter for CommonStreamingDetector Interface.

Wraps AADSDetector to provide:
- fit_score(event)
- reset()
- name()
"""

from typing import Any, Optional
from src.agentic_streaming.aads.config import AADSConfig
from src.agentic_streaming.aads.detector import AADSDetector
from src.agentic_streaming.detectors.interface import CommonStreamingDetector


class AADSAdapter(CommonStreamingDetector):
    """Adapter for the 3-stage AADS baseline detector."""

    def __init__(
        self,
        config: Optional[AADSConfig] = None,
        threshold: float = 0.50,
    ) -> None:
        super().__init__(threshold=threshold)
        self.config = config or AADSConfig()
        self._init_detector()

    def _init_detector(self) -> None:
        self.detector = AADSDetector(self.config)

    def fit_score(self, event: Any) -> float:
        """Run AADS detector on single observation."""
        return self.detector.fit_score(event)

    def reset(self) -> None:
        """Reset AADS internal clustering and density estimator."""
        self.total_processed = 0
        self._init_detector()

    def name(self) -> str:
        return "AADS"
