"""Common Streaming Detector Interface.

Defines the required standardized contract:
- fit_score(event)
- reset()
- name()
"""

from abc import ABC, abstractmethod
import time
from typing import Any, Dict, Optional
from uuid import uuid4
import numpy as np


class CommonStreamingDetector(ABC):
    """Standardized abstract interface for streaming detectors and adapters."""

    def __init__(self, threshold: float = 0.5) -> None:
        self.threshold = threshold
        self.total_processed: int = 0

    @abstractmethod
    def fit_score(self, event: Any) -> float:
        """Fit internal model on incoming observation and return anomaly score.

        Args:
            event: Scalar float, 1D numpy array, or dictionary.

        Returns:
            Normalized anomaly score in [0.0, 1.0].
        """
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset internal detector state."""
        pass

    @abstractmethod
    def name(self) -> str:
        """Return standardized string identifier of the detector."""
        pass

    def is_anomaly(self, score: float) -> bool:
        """Evaluate binary anomaly classification against threshold."""
        return bool(score >= self.threshold)

    def process_record(self, record: Any) -> Dict[str, Any]:
        """Process event and return unified result schema."""
        start_t = time.perf_counter()

        # Parse event fields
        if isinstance(record, dict):
            val = float(record["value"])
            ts = float(record.get("timestamp", time.time()))
            event_id = str(record.get("event_id", uuid4().hex))
        elif hasattr(record, "value"):
            val = float(record.value)
            ts = float(getattr(record, "timestamp", time.time()))
            event_id = str(getattr(record, "event_id", uuid4().hex))
        else:
            val = float(record)
            ts = time.time()
            event_id = uuid4().hex

        # Run fit_score
        score = self.fit_score(val)
        self.total_processed += 1
        elapsed_ms = (time.perf_counter() - start_t) * 1000.0

        return {
            "event_id": event_id,
            "timestamp": ts,
            "value": val,
            "anomaly_score": round(float(score), 6),
            "is_anomaly": self.is_anomaly(score),
            "detector_name": self.name(),
            "latency_ms": round(elapsed_ms, 4),
        }
