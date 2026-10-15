"""Continuous Real-Time Streaming Telemetry Generator

Continuously emits live telemetry metrics (1 event per second) to both
Kafka and the SQLite analytics database.
"""

import argparse
import json
import random
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


def run_continuous_stream(
    rate_hz: float = 1.0,
    anomaly_probability: float = 0.15,
    detector: str = "AADS"
):
    print("=" * 75)
    print(" 🚀 STARTING CONTINUOUS REAL-TIME TELEMETRY STREAM")
    print(f"  • Rate:            {rate_hz} event(s)/second")
    print(f"  • Anomaly Chance:  {int(anomaly_probability * 100)}%")
    print(f"  • Active Detector: {detector}")
    print(f"  • Grafana URL:     http://localhost:3000 (auto-refreshes every 5s)")
    print("=" * 75)
    print("Press Ctrl + C at any time to stop.\n")

    storage = RelationalStorageManager()
    
    producer = None
    try:
        producer = StreamingReplayProducer(rate_events_per_sec=rate_hz)
        print(" Connected to Kafka on localhost:9092")
    except Exception as e:
        print(f" Notice: Kafka connection skipped ({e}). Streaming directly to SQLite storage.")

    base_cpu = 95.0
    count = 0

    try:
        while True:
            count += 1
            now_iso = datetime.now(timezone.utc).isoformat()
            now_ts = time.time()
            evt_id = f"evt-live-{uuid4().hex[:6]}"

            # Determine whether this is a normal point or an anomaly spike
            is_anomaly = random.random() < anomaly_probability
            if is_anomaly:
                value = round(random.uniform(105.0, 120.0), 2)
            else:
                # Smooth random walk around 94 - 98
                drift = random.uniform(-0.8, 0.8)
                base_cpu = max(90.0, min(99.0, base_cpu + drift))
                value = round(base_cpu, 2)

            record = MetricRecord(
                event_id=evt_id,
                timestamp=now_ts,
                metric_id="CPU_LIVE_UTILIZATION",
                value=value,
                ground_truth=1 if is_anomaly else 0,
                dataset="LIVE_TELEMETRY",
            )

            # 1. Publish to Kafka
            if producer:
                try:
                    producer.send_record(record)
                except Exception:
                    pass

            # 2. Log event into SQLite
            storage.log_event(
                event_id=evt_id,
                timestamp=now_iso,
                metric_id=record.metric_id,
                value=record.value,
                features_json=json.dumps({"source": "live_stream", "type": "CPU"}),
            )

            # 3. Log detection
            score = min(1.0, value / 100.0)
            latency_ms = round(random.uniform(12.0, 45.0), 1)
            storage.log_detection(
                event_id=evt_id,
                detector=detector,
                score=score,
                prediction=is_anomaly,
                latency_ms=latency_ms,
                timestamp=now_iso,
            )

            # 4. If anomaly, log Agent autonomous decision
            if is_anomaly:
                action = random.choice([
                    "TRIGGER_INCIDENT_ALERT",
                    "SCALE_UP_POD_REPLICA",
                    "THROTTLE_BACKGROUND_JOBS",
                    "DYNAMIC_THRESHOLD_ADAPT"
                ])
                reason = f"CRITICAL SPIKE: CPU surged to {value}% (threshold 100%). Agent automated action '{action}' triggered."
                storage.log_agent_decision(
                    event_id=evt_id,
                    action=action,
                    reason=reason,
                    success=True,
                    latency_ms=round(random.uniform(30.0, 65.0), 1),
                    active_detector=detector,
                    timestamp=now_iso,
                )
                print(f"[{count:04d}] ⚠️ ANOMALY SPIKE: {value}%  --> Agent Action: {action}")
            else:
                print(f"[{count:04d}]  Normal metric: {value}% | Storage: Logged")

            time.sleep(1.0 / rate_hz)

    except KeyboardInterrupt:
        print("\n" + "=" * 75)
        print(f"🛑 Live streaming stopped after {count} events.")
        if producer:
            producer.close()
        print("=" * 75)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Continuous Live Stream Generator")
    parser.add_argument("--rate", type=float, default=1.0, help="Events per second (default: 1.0)")
    parser.add_argument("--anomaly-prob", type=float, default=0.15, help="Anomaly probability (default: 0.15)")
    parser.add_argument("--detector", type=str, default="AADS", help="Detector name (default: AADS)")
    args = parser.parse_args()

    run_continuous_stream(
        rate_hz=args.rate,
        anomaly_probability=args.anomaly_prob,
        detector=args.detector
    )
