"""Unit tests for Pydantic data models across the streaming and agent pipeline."""

from datetime import datetime, timezone
import json
import pytest
from pydantic import ValidationError

from src.schemas.metric import MetricRecord, MetricBatch
from src.schemas.anomaly import AnomalyEvent
from src.schemas.drift import DriftReport
from src.schemas.detector import DetectorType, DetectorScore, DetectorComparisonResult
from src.schemas.agent import AgentActionType, ToolCallRecord, AgentDecision
from src.schemas.evaluation import (
    DetectionEvaluationMetrics,
    StreamingEvaluationMetrics,
    AgentEvaluationMetrics,
    AdaptationEvaluationMetrics,
)


def test_metric_record_serialization():
    """Verify MetricRecord creates and serializes to JSON for Kafka payload."""
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    metric = MetricRecord(
        timestamp=now,
        metric_id="cpu_utilization",
        value=78.4,
        ground_truth=0,
        metadata={"server_id": "srv-01"},
    )
    json_str = metric.model_dump_json()
    data = json.loads(json_str)

    assert data["metric_id"] == "cpu_utilization"
    assert data["value"] == 78.4
    assert data["ground_truth"] == 0
    assert metric.ground_truth_label == 0
    assert data["metadata"]["server_id"] == "srv-01"

    # Reconstruct from JSON
    reconstructed = MetricRecord.model_validate_json(json_str)
    assert reconstructed.metric_id == metric.metric_id
    assert reconstructed.value == metric.value



def test_anomaly_event_creation():
    """Verify AnomalyEvent contains required context for agentic reasoning."""
    now = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
    event = AnomalyEvent(
        timestamp=now,
        metric_id="memory_usage",
        value=98.7,
        anomaly_score=4.8,
        threshold=3.0,
        window_mean=45.0,
        window_std=11.2,
        recent_window=[44.0, 46.0, 45.5, 98.7],
    )
    assert len(event.event_id) > 0
    assert event.detector_name == "AADS"
    assert event.anomaly_score == 4.8
    assert len(event.recent_window) == 4


def test_agent_decision_validation():
    """Verify AgentDecision enforces allowed actions and score bounds."""
    tool_call = ToolCallRecord(
        tool_name="check_drift",
        arguments={"metric_id": "cpu_utilization", "window_size": 100},
        output_summary="Drift confirmed p=0.001",
        duration_ms=12.5,
        success=True,
    )
    decision = AgentDecision(
        event_id="evt-12345",
        metric_id="cpu_utilization",
        action=AgentActionType.CHECK_DRIFT,
        confidence=0.92,
        reasoning="Significant variance detected with potential regime shift.",
        selected_detector=DetectorType.HSTREE,
        tool_calls=[tool_call],
        drift_detected=True,
        latency_ms=145.0,
    )

    assert decision.action == AgentActionType.CHECK_DRIFT
    assert decision.confidence == 0.92
    assert decision.selected_detector == DetectorType.HSTREE
    assert len(decision.tool_calls) == 1
    assert decision.tool_calls[0].tool_name == "check_drift"

    with pytest.raises(ValidationError):
        # Confidence must be between 0.0 and 1.0
        AgentDecision(
            event_id="evt-1",
            metric_id="m-1",
            action=AgentActionType.INVESTIGATE,
            confidence=1.5,
            reasoning="Invalid confidence",
        )


def test_detector_comparison_schema():
    """Verify DetectorComparisonResult models multi-detector consensus."""
    score_aads = DetectorScore(
        detector_name=DetectorType.AADS,
        score=0.85,
        threshold=0.7,
        is_anomaly=True,
        execution_time_ms=1.2,
    )
    score_xstream = DetectorScore(
        detector_name=DetectorType.XSTREAM,
        score=0.91,
        threshold=0.7,
        is_anomaly=True,
        execution_time_ms=3.4,
    )
    comparison = DetectorComparisonResult(
        metric_id="network_throughput",
        event_id="evt-99",
        scores={"AADS": score_aads, "xStream": score_xstream},
        consensus_is_anomaly=True,
        recommended_detector=DetectorType.XSTREAM,
        reasoning="xStream shows higher separation margin in multi-dimensional space.",
    )
    assert comparison.consensus_is_anomaly is True
    assert comparison.recommended_detector == DetectorType.XSTREAM
    assert len(comparison.scores) == 2


def test_evaluation_metrics_schemas():
    """Verify evaluation metric schemas validate values correctly."""
    det_metrics = DetectionEvaluationMetrics(
        detector_name="AADS",
        precision=0.88,
        recall=0.92,
        f1_score=0.90,
        false_positive_rate=0.03,
        total_samples=1000,
        true_positives=92,
        false_positives=12,
        true_negatives=888,
        false_negatives=8,
    )
    assert det_metrics.f1_score == 0.90

    stream_metrics = StreamingEvaluationMetrics(
        throughput_events_per_sec=4500.0,
        mean_detection_latency_ms=0.45,
        p95_detection_latency_ms=1.1,
        mean_processing_latency_ms=2.3,
        memory_usage_mb=128.5,
        window_count=50,
    )
    assert stream_metrics.throughput_events_per_sec == 4500.0
