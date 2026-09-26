"""Integration test for end-to-end streaming pipeline:
Kafka producer -> raw-metrics -> Spark streaming -> processed-metrics.
"""

import json
import time
from uuid import uuid4
import pytest
from kafka import KafkaConsumer, KafkaProducer

from src.agentic_streaming.kafka.producer import StreamingReplayProducer
from src.agentic_streaming.streaming.pipeline import SparkStreamingPipeline
from src.config.settings import get_settings
from src.schemas.metric import MetricRecord


def is_kafka_available(bootstrap_servers: str = "localhost:9092") -> bool:
    """Check if Kafka broker is reachable."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=2000,
        )
        producer.close()
        return True
    except Exception:
        return False


@pytest.mark.integration
def test_end_to_end_spark_streaming_pipeline(tmp_path):
    """Verify full end-to-end streaming flow from raw-metrics to processed-metrics."""
    settings = get_settings()
    if not is_kafka_available(settings.kafka_bootstrap_servers):
        pytest.skip("Kafka broker not reachable on localhost:9092. Skipping integration test.")

    raw_topic = f"raw-metrics-test-{uuid4().hex[:6]}"
    processed_topic = f"processed-metrics-test-{uuid4().hex[:6]}"
    checkpoint_dir = tmp_path / "spark_checkpoints"

    # Step 1: Produce 5 test events to raw_topic
    producer = StreamingReplayProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=raw_topic,
        rate_events_per_sec=0,
    )

    test_metric_id = "kpi-spark-e2e"
    test_values = [10.0, 20.0, 30.0, 40.0, 50.0]

    for i, val in enumerate(test_values):
        record = MetricRecord(
            timestamp=1600000000 + (i * 60),
            metric_id=test_metric_id,
            value=val,
            ground_truth=1 if i == 2 else 0,
        )
        producer.send_record(record)

    producer.producer.flush()
    producer.close()

    # Step 2: Run SparkStreamingPipeline for 5 records
    pipeline = SparkStreamingPipeline(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        input_topic=raw_topic,
        output_topic=processed_topic,
        checkpoint_dir=checkpoint_dir,
        window_size=5,
    )

    processed_count = pipeline.run_micro_batch_stream(
        max_records=5,
        poll_timeout_ms=3000,
        stop_on_idle=True,
    )
    assert processed_count == 5, f"Expected 5 processed records, got {processed_count}"
    assert pipeline.health.total_records_processed == 5
    assert pipeline.health.total_malformed_records == 0

    # Step 3: Consume and verify from processed_topic
    consumer = KafkaConsumer(
        processed_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"e2e-verify-{uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=5000,
    )

    consumed_records = []
    for msg in consumer:
        consumed_records.append(msg.value)
        if len(consumed_records) >= 5:
            break
    consumer.close()

    assert len(consumed_records) == 5, f"Expected 5 consumed records from {processed_topic}, got {len(consumed_records)}"

    last_record = consumed_records[-1]
    assert last_record["metric_id"] == test_metric_id
    assert last_record["value"] == 50.0
    assert last_record["rolling_count"] == 5
    # Mean of [10, 20, 30, 40, 50] is 30.0
    assert last_record["rolling_mean"] == 30.0
    assert last_record["rolling_min"] == 10.0
    assert last_record["rolling_max"] == 50.0
    assert "window_start" in last_record
    assert "window_end" in last_record
    assert "processing_timestamp" in last_record
    assert last_record["source"] == "SparkStructuredStreaming"
