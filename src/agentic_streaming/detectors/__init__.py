"""Streaming Anomaly Detectors Package.

Includes:
- AADS: Autonomous Anomaly Detection on Streams baseline
- HSTree: Half-Space Trees streaming detector
- xStream: Multi-projection density estimator
- RRCF: Robust Random Cut Forest streaming ensemble
"""

from src.agentic_streaming.detectors.base import BaseStreamingDetector
from src.agentic_streaming.detectors.hstree import HSTreeDetector
from src.agentic_streaming.detectors.xstream import XStreamDetector
from src.agentic_streaming.detectors.rrcf_detector import RRCFDetector

__all__ = [
    "BaseStreamingDetector",
    "HSTreeDetector",
    "XStreamDetector",
    "RRCFDetector",
]
