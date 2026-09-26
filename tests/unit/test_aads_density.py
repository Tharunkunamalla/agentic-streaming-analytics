"""Unit tests for Stage 1: Data-Density-Based Identification."""

import pytest
from src.agentic_streaming.aads.config import AADSConfig
from src.agentic_streaming.aads.density import DensityEstimator


def test_density_warmup_phase():
    """Verify density estimator returns nominal during warm-up phase."""
    config = AADSConfig(min_warmup_samples=10, density_z_threshold=2.5)
    estimator = DensityEstimator(config)

    # First 9 samples
    for i in range(9):
        is_candidate, density, score = estimator.update(50.0)
        assert is_candidate is False
        assert density == 1.0


def test_density_nominal_samples():
    """Verify stable nominal stream produces high density and low anomaly score."""
    config = AADSConfig(min_warmup_samples=10, density_z_threshold=2.5)
    estimator = DensityEstimator(config)

    for _ in range(30):
        is_candidate, density, score = estimator.update(50.0)

    # Stationary nominal point with small natural noise
    is_candidate, density, score = estimator.update(50.1)
    assert is_candidate is False
    assert density > 0.8
    assert score < 2.5


def test_density_potential_anomaly_detection():
    """Verify significant outlier drops density and triggers potential anomaly candidate flag."""
    config = AADSConfig(min_warmup_samples=15, density_z_threshold=2.5)
    estimator = DensityEstimator(config)

    # Baseline: values between 48.0 and 52.0
    for i in range(25):
        val = 50.0 + (1.0 if i % 2 == 0 else -1.0)
        estimator.update(val)

    # Extreme outlier (e.g. 95.0, ~40-sigma deviation)
    is_candidate, density, score = estimator.update(95.0)
    assert is_candidate is True
    assert density < 0.1
    assert score >= 2.5
