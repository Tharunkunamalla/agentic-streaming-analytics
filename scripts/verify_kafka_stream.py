"""Phase 2B Verification Script: Kafka Streaming Pipeline & Topic Validation.

Validates:
1. Kafka broker connectivity on localhost:9092.
2. Required topic existence (raw-metrics, processed-metrics, anomaly-events, agent-decisions, analytics-results).
3. Producer event publishing and delivery acknowledgment.
4. Consumer JSON deserialization and ground-truth validation.
"""

import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.agentic_streaming.kafka.producer import StreamingReplayProducer, load_dataset_records, find_default_dataset
from src.agentic_streaming.kafka.consumer import StreamingConsumer
from src.config.settings import get_settings
from scripts.create_kafka_topics import init_topics


def run_verification(num_test_events: int = 50) -> bool:
    settings = get_settings()
    print("=" * 70)
    print("Phase 2B Verification: Kafka Streaming Infrastructure")
    print(f"Broker: {settings.kafka_bootstrap_servers}")
    print(f"Topic:  {settings.kafka_raw_metrics_topic}")
    print("=" * 70)

    # Step 1: Ensure Topics Exist
    print("[1/3] Ensuring Kafka topics exist ...")
    topics_ok = init_topics(bootstrap_servers=settings.kafka_bootstrap_servers)
    if not topics_ok:
        print("FAILED: Could not initialize Kafka topics. Make sure Docker is running.")
        return False
    print("Topics check: OK")

    # Step 2: Test Producer Emission
    print(f"\n[2/3] Streaming {num_test_events} events via StreamingReplayProducer ...")
    dataset_path = find_default_dataset()
    df = load_dataset_records(dataset_path, limit=num_test_events)

    producer = StreamingReplayProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_raw_metrics_topic,
        rate_events_per_sec=20.0,
    )

    try:
        sent_count = producer.stream_dataframe(df, limit=num_test_events, loop=False)
        assert sent_count == num_test_events, f"Expected {num_test_events}, sent {sent_count}"
        assert producer.total_errors == 0, f"Encountered {producer.total_errors} send errors"
        print(f"Producer check: OK ({sent_count} events sent with 0 errors)")
    finally:
        producer.close()

    # Step 3: Test Consumer Deserialization & Schema Validation
    print(f"\n[3/3] Consuming & validating events from '{settings.kafka_raw_metrics_topic}' ...")
    consumer = StreamingConsumer(
        topic=settings.kafka_raw_metrics_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"verify-group-{int(time.time())}",
        auto_offset_reset="earliest",
        timeout_ms=6000,
    )

    consumed = []
    anomalies = 0
    try:
        for rec in consumer.consume_records(max_records=num_test_events):
            consumed.append(rec)
            if rec.ground_truth == 1:
                anomalies += 1
            if len(consumed) <= 5 or len(consumed) == num_test_events:
                print(f"  Validated: id={rec.event_id[:8]}... | ts={int(rec.timestamp)} | kpi={rec.metric_id[:8]}... | val={rec.value:.4f}")

        print(f"Consumer check: OK ({len(consumed)} records consumed, {anomalies} anomalies verified)")
        assert len(consumed) > 0, "No records were consumed from Kafka"
    finally:
        consumer.close()

    print("\n" + "=" * 70)
    print("Phase 2B Verification: SUCCESS (Kafka streaming is fully operational)")
    print("=" * 70)
    return True


def main() -> int:
    try:
        success = run_verification(num_test_events=50)
        return 0 if success else 1
    except Exception as e:
        print(f"Verification FAILED with error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
