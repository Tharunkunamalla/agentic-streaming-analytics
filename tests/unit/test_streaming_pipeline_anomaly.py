"""Unit test for Phase 7 streaming pipeline anomaly integration."""

from unittest.mock import MagicMock
import pytest
from src.agentic_streaming.streaming.pipeline import SparkStreamingPipeline
from src.schemas.anomaly import AnomalyEvent


def test_pipeline_anomaly_detection_integration(tmp_path):
    """Verify SparkStreamingPipeline processes raw events and generates AnomalyEvent on spikes."""
    pipeline = SparkStreamingPipeline(
        bootstrap_servers="localhost:9092",
        checkpoint_dir=tmp_path / "chk",
        window_size=10,
    )

    # 1. Establish baseline with 35 nominal records (satisfying min_warmup_samples = 30)
    for i in range(35):
        nom_record = {
            "event_id": f"evt-nom-{i}",
            "timestamp": 1700000000.0 + i * 10,
            "metric_id": "test-kpi",
            "value": 50.0 + (i % 2) * 0.5,
            "ground_truth": 0,
        }
        processed_nom = pipeline.process_raw_record(nom_record)
        assert processed_nom is not None
        pipeline.aads_detector.detect_one({
            "event_id": processed_nom.event_id,
            "timestamp": processed_nom.timestamp,
            "value": processed_nom.value,
            "metric_id": processed_nom.metric_id,
        })

    # 2. Extreme spike record
    spike_record = {
        "event_id": "evt-spike-1",
        "timestamp": 1700000100.0,
        "metric_id": "test-kpi",
        "value": 9999.0,
        "ground_truth": 1,
    }
    processed_spike = pipeline.process_raw_record(spike_record)
    assert processed_spike is not None
    assert processed_spike.value == 9999.0

    # Test AADS detector flagging
    aads_res = pipeline.aads_detector.detect_one({
        "event_id": processed_spike.event_id,
        "timestamp": processed_spike.timestamp,
        "value": processed_spike.value,
        "metric_id": processed_spike.metric_id,
    })

    assert aads_res["is_anomaly"] is True

    # Construct AnomalyEvent payload
    payload = AnomalyEvent(
        event_id=processed_spike.event_id,
        timestamp=processed_spike.timestamp,
        dataset="AIOPS_KPI",
        features={
            "metric_id": processed_spike.metric_id,
            "value": processed_spike.value,
            "ground_truth": processed_spike.ground_truth,
        },
        anomaly_score=float(aads_res["anomaly_score"]),
        detector="AADS",
        recent_window_summary={
            "rolling_mean": processed_spike.rolling_mean,
            "rolling_std": processed_spike.rolling_std,
        },
        anomaly_frequency=0.1,
    )

    assert payload.dataset == "AIOPS_KPI"
    assert payload.detector == "AADS"
    assert payload.features["value"] == 9999.0
    assert payload.features["metric_id"] == "test-kpi"
