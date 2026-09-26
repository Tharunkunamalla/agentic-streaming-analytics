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

    raw_topic = "raw-metrics"
    processed_topic = "processed-metrics"
    checkpoint_dir = tmp_path / "spark_checkpoints"

    # Step 1: Produce 5 test events with unique metric_id
    producer = StreamingReplayProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=raw_topic,
        rate_events_per_sec=0,
    )

    test_metric_id = f"kpi-spark-e2e-{uuid4().hex[:6]}"
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

    # Step 2: Run SparkStreamingPipeline
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
    assert processed_count >= 1, "Pipeline should process at least one record"
    assert pipeline.health.total_records_processed >= 1
    assert pipeline.health.total_malformed_records == 0

    # Step 3: Consume and verify from processed_topic using StreamingConsumer
    consumer = KafkaConsumer(
        processed_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"e2e-verify-{uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=4000,
    )

    consumed_records = []
    start_time = time.time()
    try:
        while (time.time() - start_time) < 6.0 and len(consumed_records) < 5:
            msg_dict = consumer.poll(timeout_ms=1000, max_records=5)
            if not msg_dict:
                continue
            for _tp, msgs in msg_dict.items():
                for msg in msgs:
                    rec = msg.value
                    if isinstance(rec, dict) and "rolling_mean" in rec:
                        consumed_records.append(rec)
                    if len(consumed_records) >= 5:
                        break
                if len(consumed_records) >= 5:
                    break
    finally:
        consumer.close()

    assert len(consumed_records) > 0, f"Expected consumed records from {processed_topic}"
    sample = consumed_records[0]
    assert "rolling_mean" in sample
    assert "rolling_std" in sample
    assert "rolling_count" in sample
    assert "window_start" in sample
    assert "window_end" in sample
    assert "processing_timestamp" in sample
    assert sample["source"] == "SparkStructuredStreaming"
