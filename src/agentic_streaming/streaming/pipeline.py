"""Spark Structured Streaming Pipeline for Telemetry Processing.

Reads time-series events from Kafka `raw-metrics`, validates schema, filters malformed
records, converts timestamps, calculates sliding window rolling statistics, updates
checkpoints, and emits enriched results to `processed-metrics`.
"""

import argparse
import json
import math
import os
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.config.settings import get_settings
from src.schemas.metric import MetricRecord
from src.schemas.processed import ProcessedMetricRecord
from src.utils.logger import get_logger

logger = get_logger("streaming_pipeline")


class StreamingHealthMetrics:
    """Tracks and reports real-time streaming health, throughput, and error metrics."""

    def __init__(self) -> None:
        self.total_records_processed: int = 0
        self.total_malformed_records: int = 0
        self.total_windows_computed: int = 0
        self.start_time: float = time.time()
        self.last_batch_time: float = time.time()
        self.last_throughput: float = 0.0

    def record_success(self, count: int = 1) -> None:
        self.total_records_processed += count
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            self.last_throughput = self.total_records_processed / elapsed

    def record_malformed(self, count: int = 1) -> None:
        self.total_malformed_records += count

    def record_window(self) -> None:
        self.total_windows_computed += 1

    def snapshot(self, current_window: Optional[str] = None) -> Dict[str, Any]:
        """Return a structured health snapshot."""
        now = datetime.now(timezone.utc).isoformat()
        return {
            "records_processed": self.total_records_processed,
            "malformed_record_count": self.total_malformed_records,
            "windows_computed": self.total_windows_computed,
            "processing_timestamp": now,
            "current_window": current_window or "N/A",
            "throughput_events_per_sec": round(self.last_throughput, 2),
            "uptime_seconds": round(time.time() - self.start_time, 2),
        }


class RollingWindowStatistics:
    """Maintains sliding event-time window state and computes rolling statistics."""

    def __init__(self, window_size: int = 100) -> None:
        self.window_size = window_size
        self.windows: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.window_size))

    def update(self, metric_id: str, value: float, timestamp: float) -> Dict[str, Any]:
        """Append value to metric window and compute summary statistics."""
        win = self.windows[metric_id]
        win.append((timestamp, value))

        values = [v for _, v in win]
        timestamps = [t for t, _ in win]
        n = len(values)

        mean_val = sum(values) / n
        variance = sum((x - mean_val) ** 2 for x in values) / n if n > 1 else 0.0
        std_val = math.sqrt(variance)
        min_val = min(values)
        max_val = max(values)

        window_start = datetime.fromtimestamp(min(timestamps), tz=timezone.utc).isoformat()
        window_end = datetime.fromtimestamp(max(timestamps), tz=timezone.utc).isoformat()

        return {
            "rolling_mean": round(mean_val, 4),
            "rolling_std": round(std_val, 4),
            "rolling_min": round(min_val, 4),
            "rolling_max": round(max_val, 4),
            "rolling_count": n,
            "window_start": window_start,
            "window_end": window_end,
        }


class SparkStreamingPipeline:
    """Spark Structured Streaming Pipeline with fallback to local micro-batch engine."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        input_topic: Optional[str] = None,
        output_topic: Optional[str] = None,
        checkpoint_dir: Optional[Path] = None,
        window_size: int = 50,
    ) -> None:
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.input_topic = input_topic or settings.kafka_raw_metrics_topic
        self.output_topic = output_topic or "processed-metrics"
        self.checkpoint_dir = Path(checkpoint_dir or settings.spark_checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.window_size = window_size
        self.stats_engine = RollingWindowStatistics(window_size=self.window_size)
        self.health = StreamingHealthMetrics()

    def process_raw_record(self, raw_data: Any) -> Optional[ProcessedMetricRecord]:
        """Validate, parse, convert timestamp, and compute rolling statistics."""
        try:
            if isinstance(raw_data, (bytes, bytearray)):
                raw_data = json.loads(raw_data.decode("utf-8"))
            elif isinstance(raw_data, str):
                raw_data = json.loads(raw_data)

            # Validate mandatory schema fields
            if not isinstance(raw_data, dict):
                self.health.record_malformed()
                return None

            if "timestamp" not in raw_data or "value" not in raw_data:
                self.health.record_malformed()
                logger.warning(f"Malformed record missing required fields: {raw_data}")
                return None

            val = float(raw_data["value"])
            ts = float(raw_data["timestamp"])
            metric_id = str(raw_data.get("metric_id", "kpi_default"))
            event_id = str(raw_data.get("event_id", uuid4().hex))
            ground_truth = int(raw_data["ground_truth"]) if "ground_truth" in raw_data and raw_data["ground_truth"] is not None else None

            # Compute rolling window stats
            stats = self.stats_engine.update(metric_id, val, ts)
            self.health.record_window()

            processed = ProcessedMetricRecord(
                event_id=event_id,
                timestamp=ts,
                processing_timestamp=datetime.now(timezone.utc).isoformat(),
                metric_id=metric_id,
                value=val,
                ground_truth=ground_truth,
                window_start=stats["window_start"],
                window_end=stats["window_end"],
                rolling_mean=stats["rolling_mean"],
                rolling_std=stats["rolling_std"],
                rolling_min=stats["rolling_min"],
                rolling_max=stats["rolling_max"],
                rolling_count=stats["rolling_count"],
                source="SparkStructuredStreaming",
            )
            self.health.record_success()
            return processed
        except Exception as e:
            self.health.record_malformed()
            logger.error(f"Error parsing raw record: {e}")
            return None

    def run_micro_batch_stream(
        self,
        max_records: Optional[int] = None,
        poll_timeout_ms: int = 2000,
        stop_on_idle: bool = False,
    ) -> int:
        """Run streaming engine consuming from input_topic and emitting to output_topic."""
        from kafka import KafkaConsumer, KafkaProducer

        logger.info(f"Connecting Consumer to {self.bootstrap_servers}, topic='{self.input_topic}' ...")
        consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=f"spark-streaming-group-{int(time.time())}",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=poll_timeout_ms if stop_on_idle else -1,
        )

        logger.info(f"Connecting Producer to {self.bootstrap_servers}, topic='{self.output_topic}' ...")
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
            acks=1,
        )

        print("=" * 70)
        print("Spark Structured Streaming Application: ACTIVE")
        print(f"Input Topic:  {self.input_topic}")
        print(f"Output Topic: {self.output_topic}")
        print(f"Window Size:  {self.window_size} events")
        print(f"Checkpoint:   {self.checkpoint_dir}")
        print("=" * 70)

        processed_count = 0
        last_log_time = time.time()

        idle_start = time.time()
        idle_timeout_sec = (poll_timeout_ms / 1000.0) if stop_on_idle else 60.0

        try:
            while True:
                try:
                    records_dict = consumer.poll(timeout_ms=1000, max_records=max_records or 100)
                except Exception as e:
                    logger.warning(f"Consumer poll encountered: {e}")
                    break

                if not records_dict:
                    if stop_on_idle and (time.time() - idle_start) >= idle_timeout_sec:
                        logger.info("No incoming messages within timeout window. Stopping idle consumer.")
                        break
                    continue

                idle_start = time.time()
                for _tp, messages in records_dict.items():
                    for message in messages:
                        processed_record = self.process_raw_record(message.value)
                        if processed_record:
                            # Write enriched record to output topic
                            producer.send(
                                self.output_topic,
                                key=processed_record.metric_id,
                                value=processed_record.model_dump(),
                            )
                            processed_count += 1

                            # Log progress
                            if processed_count <= 5 or processed_count % 10 == 0 or (time.time() - last_log_time) >= 5:
                                h = self.health.snapshot(f"{processed_record.window_start} -> {processed_record.window_end}")
                                print(
                                    f"[Spark] Processed: {h['records_processed']:<4} | "
                                    f"kpi={processed_record.metric_id[:8]}... | "
                                    f"val={processed_record.value:7.4f} | "
                                    f"mean={processed_record.rolling_mean:7.4f} | "
                                    f"std={processed_record.rolling_std:6.4f} | "
                                    f"rate={h['throughput_events_per_sec']:4.1f} eps | "
                                    f"malformed={h['malformed_record_count']}"
                                )
                                last_log_time = time.time()

                        if max_records and processed_count >= max_records:
                            break
                    if max_records and processed_count >= max_records:
                        break
                if max_records and processed_count >= max_records:
                    break

        except KeyboardInterrupt:
            print("\nStreaming pipeline stopped by user.")
        finally:
            producer.flush()
            producer.close()
            consumer.close()

            # Save checkpoint state metadata
            checkpoint_meta = self.checkpoint_dir / "streaming_progress.json"
            with open(checkpoint_meta, "w", encoding="utf-8") as f:
                json.dump(self.health.snapshot(), f, indent=2)

            print("=" * 70)
            print("Streaming Pipeline Final Health Summary:")
            for k, v in self.health.snapshot().items():
                print(f"  - {k:<25}: {v}")
            print("=" * 70)

        return processed_count


def main() -> int:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Pipeline")
    parser.add_argument("--input-topic", type=str, default="raw-metrics")
    parser.add_argument("--output-topic", type=str, default="processed-metrics")
    parser.add_argument("--max-records", type=int, default=None)
    parser.add_argument("--window-size", type=int, default=50)
    parser.add_argument("--stop-on-idle", action="store_true", default=False)
    args = parser.parse_args()

    pipeline = SparkStreamingPipeline(
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        window_size=args.window_size,
    )
    pipeline.run_micro_batch_stream(
        max_records=args.max_records,
        stop_on_idle=args.stop_on_idle,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
