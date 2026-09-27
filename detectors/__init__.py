"""Root detectors package forwarding to src.agentic_streaming.detectors."""

from src.agentic_streaming.detectors.interface import CommonStreamingDetector
from src.agentic_streaming.detectors.xstream_adapter import XStreamAdapter
from src.agentic_streaming.detectors.hstree_adapter import HSTreeAdapter
from src.agentic_streaming.detectors.rrcf_adapter import RRCFAdapter
from src.agentic_streaming.detectors.aads_adapter import AADSAdapter

__all__ = [
    "CommonStreamingDetector",
    "XStreamAdapter",
    "HSTreeAdapter",
    "RRCFAdapter",
    "AADSAdapter",
]
