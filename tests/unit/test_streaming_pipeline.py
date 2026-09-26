"""Unit tests for Spark Structured Streaming processing, window statistics, and health metrics."""

import json
from pathlib import Path
import pytest

from src.agentic_streaming.streaming.pipeline import (
    RollingWindowStatistics,
    StreamingHealthMetrics,
    SparkStreamingPipeline,
)
from src.schemas.processed import ProcessedMetricRecord


def test_rolling_window_statistics_calculation():
    """Verify window statistics engine calculates mean, std, min, max correctly."""
    engine = RollingWindowStatistics(window_size=5)

    # Feed numbers 1, 2, 3, 4, 5
    for i in range(1, 6):
        res = engine.update("kpi-1", float(i), 1000.0 + i)

    assert res["rolling_count"] == 5
    assert res["rolling_mean"] == 3.0
    assert res["rolling_min"] == 1.0
    assert res["rolling_max"] == 5.0
    assert round(res["rolling_std"], 2) == 1.41

    # Feed 6th element (window slide, should drop 1)
    res = engine.update("kpi-1", 6.0, 1006.0)
    assert res["rolling_count"] == 5
    assert res["rolling_mean"] == 4.0
    assert res["rolling_min"] == 2.0
    assert res["rolling_max"] == 6.0


def test_streaming_health_metrics():
    """Verify health metric counters, throughput calculation, and snapshot structure."""
    health = StreamingHealthMetrics()
    health.record_success(count=10)
    health.record_malformed(count=2)
    health.record_window()

    snap = health.snapshot(current_window="2026-01-01 -> 2026-01-02")
    assert snap["records_processed"] == 10
    assert snap["malformed_record_count"] == 2
    assert snap["windows_computed"] == 1
    assert snap["current_window"] == "2026-01-01 -> 2026-01-02"
    assert "throughput_events_per_sec" in snap
    assert "processing_timestamp" in snap


def test_pipeline_process_valid_record(tmp_path):
    """Verify processing valid raw records produces enriched ProcessedMetricRecord."""
    pipeline = SparkStreamingPipeline(checkpoint_dir=tmp_path / "checkpoints", window_size=10)

    raw_payload = {
        "event_id": "evt-001",
        "timestamp": 1500000000.0,
        "metric_id": "kpi_cpu",
        "value": 85.5,
        "ground_truth": 1,
        "dataset": "AIOPS_KPI",
    }

    processed = pipeline.process_raw_record(raw_payload)
    assert processed is not None
    assert isinstance(processed, ProcessedMetricRecord)
    assert processed.value == 85.5
    assert processed.metric_id == "kpi_cpu"
    assert processed.rolling_mean == 85.5
    assert processed.rolling_count == 1
    assert processed.ground_truth == 1
    assert pipeline.health.total_records_processed == 1
    assert pipeline.health.total_malformed_records == 0


def test_pipeline_safe_malformed_record_handling(tmp_path):
    """Verify malformed records are safely rejected and increment error counter."""
    pipeline = SparkStreamingPipeline(checkpoint_dir=tmp_path / "checkpoints")

    # Missing value
    res1 = pipeline.process_raw_record({"timestamp": 12345, "metric_id": "test"})
    assert res1 is None

    # Invalid non-json or corrupt data
    res2 = pipeline.process_raw_record("corrupt string that is not json")
    assert res2 is None

    # Invalid type
    res3 = pipeline.process_raw_record(12345)
    assert res3 is None

    assert pipeline.health.total_malformed_records == 3
    assert pipeline.health.total_records_processed == 0
