"""Unit tests for the integrated 3-stage AADSDetector."""

import pytest
from src.agentic_streaming.aads.config import AADSConfig
from src.agentic_streaming.aads.detector import AADSDetector
from src.schemas.metric import MetricRecord


def test_detector_output_contract():
    """Verify detect_one returns all required fields specified by the project specification."""
    detector = AADSDetector()
    record = {
        "event_id": "test-evt-001",
        "timestamp": 1600000000.0,
        "value": 45.2,
        "metric_id": "cpu-01",
    }

    result = detector.detect_one(record)

    required_keys = [
        "event_id",
        "timestamp",
        "anomaly_score",
        "is_anomaly",
        "detector_name",
        "detector_version",
        "processing_time_ms",
    ]

    for key in required_keys:
        assert key in result, f"Result must contain required key: '{key}'"

    assert result["event_id"] == "test-evt-001"
    assert result["timestamp"] == 1600000000.0
    assert result["detector_name"] == "AADS"
    assert result["detector_version"] == "1.0.0-kbs2024"
    assert isinstance(result["is_anomaly"], bool)
    assert isinstance(result["processing_time_ms"], float)


def test_detector_with_pydantic_metric_record():
    """Verify detector processes Pydantic MetricRecord objects directly."""
    detector = AADSDetector()
    record = MetricRecord(
        timestamp=1600000000.0,
        metric_id="kpi-test",
        value=30.0,
    )
    result = detector.detect_one(record)
    assert result["metric_id"] == "kpi-test"
    assert result["value"] == 30.0


def test_detector_nominal_and_anomaly_sequence():
    """Verify detector establishes nominal baseline and flags isolated anomaly spike."""
    config = AADSConfig(min_warmup_samples=15, density_z_threshold=2.5, max_minor_cluster_size=2)
    detector = AADSDetector(config)

    # 30 stationary observations around 10.0
    for i in range(30):
        val = 10.0 + (0.5 if i % 2 == 0 else -0.5)
        res = detector.detect_one({"value": val, "timestamp": 1000 + i, "metric_id": "m1"})
        assert res["is_anomaly"] is False

    # Anomaly spike: 80.0
    res_spike = detector.detect_one({"value": 80.0, "timestamp": 1031, "metric_id": "m1"})
    assert res_spike["is_anomaly"] is True
    assert res_spike["anomaly_score"] >= 2.5


def test_detector_multi_stream_isolation():
    """Verify separate metrics do not contaminate each other's density statistics."""
    detector = AADSDetector(AADSConfig(min_warmup_samples=10))

    # Metric A has mean ~100
    for i in range(20):
        detector.detect_one({"value": 100.0, "timestamp": i, "metric_id": "metric_A"})

    # Metric B has mean ~1
    for i in range(20):
        detector.detect_one({"value": 1.0, "timestamp": i, "metric_id": "metric_B"})

    # Check that 100.0 is normal for metric_A, but anomalous for metric_B
    res_a = detector.detect_one({"value": 100.0, "timestamp": 21, "metric_id": "metric_A"})
    res_b = detector.detect_one({"value": 100.0, "timestamp": 21, "metric_id": "metric_B"})

    assert res_a["is_anomaly"] is False
    assert res_b["is_anomaly"] is True
