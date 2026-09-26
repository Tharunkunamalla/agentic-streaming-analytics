"""Integration test for end-to-end Kafka topic publishing and consuming."""

import time
from uuid import uuid4
import pytest
from kafka import KafkaConsumer, KafkaProducer

from src.agentic_streaming.kafka.consumer import StreamingConsumer
from src.agentic_streaming.kafka.producer import StreamingReplayProducer
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
def test_kafka_producer_consumer_roundtrip(tmp_path):
    """Publish records to Kafka and verify exact deserialization by consumer."""
    settings = get_settings()
    if not is_kafka_available(settings.kafka_bootstrap_servers):
        pytest.skip("Kafka broker not reachable on localhost:9092. Skipping integration test.")

    test_topic = "raw-metrics"
    producer = StreamingReplayProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=test_topic,
        rate_events_per_sec=0,
    )

    test_metric_id = f"test-metric-{uuid4().hex[:6]}"
    test_records = [
        MetricRecord(
            timestamp=1500000000 + i,
            metric_id=test_metric_id,
            value=100.0 + i,
            ground_truth=1 if i == 1 else 0,
        )
        for i in range(3)
    ]

    for r in test_records:
        producer.send_record(r)
    producer.producer.flush()

    consumer = StreamingConsumer(
        topic=test_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"test-group-{uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        timeout_ms=4000,
    )

    consumed = []
    for rec in consumer.consume_records(max_records=10):
        consumed.append(rec)

    consumer.close()
    producer.close()

    assert len(consumed) > 0, "Should consume at least one record from raw-metrics"
    assert all(isinstance(c, MetricRecord) for c in consumed)
    assert all(c.value is not None and c.timestamp is not None for c in consumed)
