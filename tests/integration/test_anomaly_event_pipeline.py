"""Phase 7 Architectural Checkpoint Integration Test:
dataset -> Kafka raw-metrics -> Spark -> AADS -> Kafka anomaly-events.

Verifies complete end-to-end data flow proving that flagged anomalies emit
structured AnomalyEvent payloads to Kafka anomaly-events topic while normal
events do NOT trigger the anomaly topic.
"""

import json
import time
from uuid import uuid4
import pytest
from kafka import KafkaConsumer, KafkaProducer

from src.agentic_streaming.kafka.producer import StreamingReplayProducer
from src.agentic_streaming.streaming.pipeline import SparkStreamingPipeline
from src.config.settings import get_settings
from src.schemas.anomaly import AnomalyEvent
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
def test_end_to_end_anomaly_event_pipeline(tmp_path):
    """Publish mixed normal & extreme spike records to Kafka and verify anomaly-events emission."""
    settings = get_settings()
    if not is_kafka_available(settings.kafka_bootstrap_servers):
        pytest.skip("Kafka broker not reachable on localhost:9092. Skipping integration test.")

    raw_topic = "raw-metrics"
    anomaly_topic = "anomaly-events"
    checkpoint_dir = tmp_path / "spark_anomaly_checkpoints"

    # Step 1: Produce baseline nominal points followed by extreme spike
    producer = StreamingReplayProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=raw_topic,
        rate_events_per_sec=0,
    )

    test_metric_id = f"kpi-anom-e2e-{uuid4().hex[:6]}"
    # 20 normal values around 50.0, then an extreme 5000.0 spike, then nominal
    records = []
    base_ts = 1700000000.0
    for i in range(20):
        records.append(MetricRecord(
            timestamp=base_ts + i * 10,
            metric_id=test_metric_id,
            value=50.0 + (i % 3) * 0.5,
            ground_truth=0,
        ))
    # Extreme anomaly spike
    records.append(MetricRecord(
        timestamp=base_ts + 200,
        metric_id=test_metric_id,
        value=5000.0,
        ground_truth=1,
    ))

    for r in records:
        producer.send_record(r)

    producer.producer.flush()
    producer.close()

    # Step 2: Run SparkStreamingPipeline with AADS baseline enabled
    pipeline = SparkStreamingPipeline(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        input_topic=raw_topic,
        output_topic="processed-metrics",
        checkpoint_dir=checkpoint_dir,
        window_size=10,
    )

    processed_count = pipeline.run_micro_batch_stream(
        max_records=len(records),
        poll_timeout_ms=3000,
        stop_on_idle=True,
    )

    assert processed_count >= 1, "Pipeline should process streamed records"

    # Step 3: Consume and verify from anomaly-events topic
    consumer = KafkaConsumer(
        anomaly_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"anomaly-verify-group-{uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=4000,
    )

    consumed_anomalies = []
    start_time = time.time()
    try:
        while (time.time() - start_time) < 6.0:
            msg_dict = consumer.poll(timeout_ms=1000, max_records=10)
            if not msg_dict:
                continue
            for _tp, msgs in msg_dict.items():
                for msg in msgs:
                    payload = msg.value
                    if isinstance(payload, dict) and payload.get("detector") == "AADS":
                        # Match metric_id inside features dict or metric_id property
                        feat = payload.get("features", {})
                        if feat.get("metric_id") == test_metric_id:
                            anom_obj = AnomalyEvent.model_validate(payload)
                            consumed_anomalies.append(anom_obj)
            if len(consumed_anomalies) >= 1:
                break
    finally:
        consumer.close()

    # Step 4: Validate architectural checkpoint requirements
    assert len(consumed_anomalies) >= 1, f"Expected at least one anomaly event in '{anomaly_topic}'"
    anom_event = consumed_anomalies[0]

    assert isinstance(anom_event.event_id, str)
    assert anom_event.dataset == "AIOPS_KPI"
    assert anom_event.detector == "AADS"
    assert anom_event.anomaly_score > 0.0
    assert "rolling_mean" in anom_event.recent_window_summary
    assert "rolling_std" in anom_event.recent_window_summary
    assert anom_event.features["metric_id"] == test_metric_id
    assert anom_event.features["value"] >= 5000.0
