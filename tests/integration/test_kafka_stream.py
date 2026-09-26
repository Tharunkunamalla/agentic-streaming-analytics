"""Integration test for end-to-end Kafka topic publishing and consuming."""

import time
import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from src.agentic_streaming.kafka.consumer import StreamingConsumer
from src.agentic_streaming.kafka.producer import StreamingReplayProducer, load_dataset_records
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

    # Emit 5 test records
    test_records = [
        MetricRecord(
            timestamp=1500000000 + i,
            metric_id="kpi-integration-test",
            value=100.0 + i,
            ground_truth=1 if i == 2 else 0,
        )
        for i in range(5)
    ]

    for r in test_records:
        producer.send_record(r)
    producer.producer.flush()

    # Consume from latest / earliest
    consumer = StreamingConsumer(
        topic=test_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"test-group-{int(time.time())}",
        auto_offset_reset="earliest",
        timeout_ms=5000,
    )

    consumed = []
    for rec in consumer.consume_records(max_records=5):
        if rec.metric_id == "kpi-integration-test":
            consumed.append(rec)

    consumer.close()
    producer.close()

    assert len(consumed) >= 1
    assert any(c.value == 102.0 and c.ground_truth == 1 for c in consumed)
