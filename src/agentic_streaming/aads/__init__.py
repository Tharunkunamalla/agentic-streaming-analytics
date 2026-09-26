"""AADS (Autonomous Anomaly Detection for Streaming Data) Package.

Grounded in:
Basheer et al., 'Autonomous Anomaly Detection for Streaming Data',
Knowledge-Based Systems, Vol. 284, 2024.
"""

from src.agentic_streaming.aads.clustering import (
    AutonomousDataPartitioner,
    MicroCluster,
)
from src.agentic_streaming.aads.config import AADSConfig
from src.agentic_streaming.aads.density import DensityEstimator
from src.agentic_streaming.aads.detector import AADSDetector
from src.agentic_streaming.aads.evaluator import AADSEvaluator

__all__ = [
    "AADSConfig",
    "DensityEstimator",
    "AutonomousDataPartitioner",
    "MicroCluster",
    "AADSDetector",
    "AADSEvaluator",
]
