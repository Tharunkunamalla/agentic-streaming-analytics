"""Abstract base class and contract for all streaming anomaly detectors."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple


class BaseStreamingDetector(ABC):
    """Unified interface for online streaming anomaly detectors."""

    def __init__(self, name: str, version: str = "1.0.0") -> None:
        self.name = name
        self.version = version
        self.total_processed: int = 0

    @abstractmethod
    def score(self, value: float, timestamp: Optional[float] = None) -> float:
        """Compute continuous anomaly score for an incoming streaming value.

        Args:
            value: Float metric value.
            timestamp: Optional event unix timestamp.

        Returns:
            Normalized or comparative anomaly score (higher indicates more anomalous).
        """
        pass

    @abstractmethod
    def update(self, value: float, timestamp: Optional[float] = None) -> None:
        """Update internal streaming models or trees with the new observation.

        Args:
            value: Float metric value.
            timestamp: Optional event unix timestamp.
        """
        pass

    def process(self, value: float, timestamp: Optional[float] = None) -> Tuple[float, bool]:
        """Score and update in a single atomic step.

        Args:
            value: Float metric value.
            timestamp: Optional event unix timestamp.

        Returns:
            Tuple of (anomaly_score, is_anomaly).
        """
        score_val = self.score(value, timestamp)
        self.update(value, timestamp)
        self.total_processed += 1
        is_anom = self.is_anomaly(score_val)
        return score_val, is_anom

    @abstractmethod
    def is_anomaly(self, score: float) -> bool:
        """Determine boolean anomaly label from raw score."""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset internal model state to initial condition."""
        pass

    def get_metadata(self) -> Dict[str, Any]:
        """Return diagnostic metadata and hyperparameter settings."""
        return {
            "name": self.name,
            "version": self.version,
            "total_processed": self.total_processed,
        }
