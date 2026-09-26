"""Unit tests for streaming anomaly detectors (HSTree, xStream, RRCF)."""

import pytest
import numpy as np

from src.agentic_streaming.detectors.hstree import HSTreeDetector
from src.agentic_streaming.detectors.xstream import XStreamDetector
from src.agentic_streaming.detectors.rrcf_detector import RRCFDetector


def test_hstree_streaming_behavior():
    """Verify HSTree processes normal baseline and flags severe spikes."""
    detector = HSTreeDetector(n_trees=15, max_depth=6, window_size=50, threshold=0.7)

    # Train on stationary normal stream
    for i in range(100):
        score, is_anom = detector.process(10.0 + (i % 3) * 0.5)
        assert 0.0 <= score <= 1.0

    # Test an extreme outlier
    spike_score, is_spike_anom = detector.process(500.0)
    assert 0.0 <= spike_score <= 1.0
    assert spike_score > 0.5


def test_xstream_streaming_behavior():
    """Verify xStream processes normal baseline and adapts."""
    detector = XStreamDetector(n_chains=10, depth=4, window_size=50, threshold=0.7)

    for i in range(60):
        score, is_anom = detector.process(50.0 + (i % 4) * 0.2)
        assert 0.0 <= score <= 1.0

    # Inject extreme spike
    spike_score, is_spike_anom = detector.process(9999.0)
    assert 0.0 <= spike_score <= 1.0
    assert spike_score > 0.4


def test_rrcf_streaming_behavior():
    """Verify RRCF builds trees, computes CoDisp, and detects outliers."""
    detector = RRCFDetector(num_trees=10, tree_size=50, threshold=0.6)

    for i in range(60):
        score, is_anom = detector.process(20.0 + (i % 2) * 0.5)
        assert 0.0 <= score <= 1.0

    # Inject spike
    spike_score, is_spike_anom = detector.process(1000.0)
    assert 0.0 <= spike_score <= 1.0
    assert spike_score > 0.4


def test_detectors_reset():
    """Verify reset clears internal state."""
    for cls in [HSTreeDetector, XStreamDetector, RRCFDetector]:
        det = cls()
        det.process(10.0)
        assert det.total_processed == 1
        det.reset()
        assert det.total_processed == 0
