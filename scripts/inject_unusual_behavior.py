"""Script to inject synthetic unusual behavior (CPU/Memory anomaly spikes) 
into Kafka raw-metrics topic and the streaming analytics database for Grafana UI visualization.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.agentic_streaming.storage.relational_storage import RelationalStorageManager
from src.agentic_streaming.kafka.producer import StreamingReplayProducer
from src.schemas.metric import MetricRecord


def inject_anomaly_spike(
    metric_name: str = "CPU",
    value: float = 95.2,
    detector: str = "AADS",
    num_spikes: int = 5,
    publish_to_kafka: bool = True,
) -> None:
    """Inject unusual metric behavior into both Kafka broker and SQLite storage."""
    print("=" * 70)
    print(f"Injecting Unusual Behavior Spike into UI...")
    print(f"  Metric:   {metric_name}")
    print(f"  Value:    {value}")
    print(f"  Detector: {detector}")
    print(f"  Spikes:   {num_spikes}")
    print("=" * 70)

    storage = RelationalStorageManager()

    producer = None
    if publish_to_kafka:
        try:
            producer = StreamingReplayProducer(rate_events_per_sec=10.0)
            print("Connected to Kafka producer at localhost:9092")
        except Exception as e:
            print(f"Notice: Kafka producer connection skipped ({e}). Direct DB injection will proceed.")

    for i in range(1, num_spikes + 1):
        now_iso = datetime.now(timezone.utc).isoformat()
        now_ts = time.time()
        evt_id = f"evt-spike-{uuid4().hex[:6]}"

        # Create Metric Payload
        record = MetricRecord(
            event_id=evt_id,
            timestamp=now_ts,
            metric_id=f"{metric_name}_CRITICAL_SPIKE",
            value=value + (i * 0.5),  # Slight variance per spike point
            ground_truth=1,
            dataset="SYNTHETIC_ANOMALY_INJECTOR",
        )

        # 1. Publish to Kafka raw-metrics topic
        if producer:
            try:
                producer.send_record(record)
                print(f"[{i}/{num_spikes}] Emitted spike event to Kafka topic 'raw-metrics' (Value={record.value:.1f})")
            except Exception as exc:
                print(f"Failed to publish to Kafka: {exc}")

        # 2. Log event into relational database for Grafana UI
        storage.log_event(
            event_id=evt_id,
            timestamp=now_iso,
            metric_id=record.metric_id,
            value=record.value,
            features_json=json.dumps({"source": "manual_injector", "metric": metric_name}),
        )

        # 3. Log detection result (ANOMALY = True)
        storage.log_detection(
            event_id=evt_id,
            detector=detector,
            score=min(1.0, record.value / 100.0),
            prediction=True,
            latency_ms=1.1,
            timestamp=now_iso,
        )

        # 4. Log agent decision
        storage.log_agent_decision(
            event_id=evt_id,
            action="TRIGGER_INCIDENT_ALERT",
            reason=f"CRITICAL ANOMALY: {metric_name} surge at {record.value:.1f}% exceeds normal threshold! Agent initiated automated triage.",
            success=True,
            latency_ms=38.4,
            active_detector=detector,
            timestamp=now_iso,
        )

        time.sleep(0.5)

    if producer:
        producer.close()

    print("\n" + "=" * 70)
    print("SUCCESS: Synthetic unusual behavior injected!")
    print("Open Grafana Dashboard at http://localhost:3000 to see live metrics updated.")
    print("=" * 70)


def main() -> int:
    parser = argparse.ArgumentParser(description="Inject unusual behavior into Grafana UI & Kafka")
    parser.add_argument("--metric", type=str, default="CPU", help="Metric name (default: CPU)")
    parser.add_argument("--value", type=float, default=95.2, help="Anomalous spike value (default: 95.2)")
    parser.add_argument("--detector", type=str, default="AADS", help="Detector name (default: AADS)")
    parser.add_argument("--spikes", type=int, default=5, help="Number of spike events (default: 5)")

    args = parser.parse_args()
    inject_anomaly_spike(
        metric_name=args.metric,
        value=args.value,
        detector=args.detector,
        num_spikes=args.spikes,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
