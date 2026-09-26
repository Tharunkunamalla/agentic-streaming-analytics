"""Kafka Time-Series Replay Producer for StreamAD / AIOPS_KPI Telemetry.

Replays time-series rows row-by-row into the Kafka `raw-metrics` topic with
configurable stream rates, deterministic batching, timestamp preservation,
and robust schema validation.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Generator
from uuid import uuid4
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.config.settings import get_settings
from src.schemas.metric import MetricRecord
from src.utils.logger import get_logger

logger = get_logger("kafka_producer")


def load_dataset_records(
    csv_path: Path,
    kpi_id: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Load and prepare dataframe rows for replay."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Dataset not found at {csv_path}. Run dataset preparation first.")

    logger.info(f"Loading dataset from {csv_path.name} ...")
    df = pd.read_csv(csv_path)

    if kpi_id:
        df = df[df["kpi_id"] == kpi_id].copy()
        logger.info(f"Filtered for KPI '{kpi_id}', remaining rows: {len(df):,}")

    if limit and limit > 0:
        df = df.iloc[:limit].copy()
        logger.info(f"Applied limit: {limit:,} records")

    return df


class StreamingReplayProducer:
    """Kafka Replay Producer that streams time series events at configurable rates."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        rate_events_per_sec: float = 10.0,
        acks: str | int = 1,
    ) -> None:
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_raw_metrics_topic
        self.rate = float(os.getenv("STREAM_RATE", rate_events_per_sec))
        self.acks = acks
        self.producer = self._create_producer()
        self.total_sent = 0
        self.total_acknowledged = 0
        self.total_errors = 0

    def _create_producer(self) -> KafkaProducer:
        """Initialize KafkaProducer with JSON serialization and retry policies."""
        logger.info(f"Connecting to Kafka at {self.bootstrap_servers} for topic '{self.topic}' ...")
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
            acks=self.acks,
            retries=3,
            retry_backoff_ms=200,
            request_timeout_ms=15000,
        )

    def _on_send_success(self, record_metadata: Any) -> None:
        """Asynchronous delivery confirmation callback."""
        self.total_acknowledged += 1

    def _on_send_error(self, exc: Exception) -> None:
        """Asynchronous delivery failure callback."""
        self.total_errors += 1
        logger.error(f"Failed to deliver record to Kafka: {exc}")

    def send_record(self, metric_record: MetricRecord) -> None:
        """Serialize and publish a single MetricRecord to the Kafka topic."""
        payload = metric_record.model_dump()
        key = metric_record.metric_id

        future = self.producer.send(
            self.topic,
            key=key,
            value=payload,
        )
        future.add_callback(self._on_send_success)
        future.add_errback(self._on_send_error)
        self.total_sent += 1

    def stream_dataframe(
        self,
        df: pd.DataFrame,
        limit: int | None = None,
        loop: bool = False,
    ) -> int:
        """Replay rows from a DataFrame into Kafka with rate pacing."""
        total_rows = len(df)
        max_events = limit if limit and limit > 0 else total_rows
        sleep_interval = (1.0 / self.rate) if self.rate > 0 else 0.0

        print("=" * 70)
        print(f"Starting Kafka Stream Replay to topic '{self.topic}'")
        print(f"Broker: {self.bootstrap_servers} | Rate: {self.rate} events/sec | Total: {max_events}")
        print("=" * 70)

        count = 0
        iteration = 1
        start_time = time.time()

        try:
            while True:
                for idx, row in df.iterrows():
                    count += 1
                    record = MetricRecord(
                        event_id=str(uuid4()),
                        timestamp=float(row["timestamp"]),
                        ingestion_timestamp=datetime.now(timezone.utc).isoformat(),
                        metric_id=str(row.get("kpi_id", "kpi_default")),
                        value=float(row["value"]),
                        ground_truth=int(row["ground_truth"]) if "ground_truth" in row and pd.notna(row["ground_truth"]) else None,
                        dataset=str(row.get("dataset", "AIOPS_KPI")),
                        source=str(row.get("source", "StreamAD")),
                    )

                    self.send_record(record)

                    # Print formatted real-time event log
                    label_str = f"label={record.ground_truth}" if record.ground_truth is not None else "label=N/A"
                    print(
                        f"event {count:<5} | ts={int(record.timestamp)} | kpi={record.metric_id[:8]}... | "
                        f"val={record.value:8.4f} | {label_str}"
                    )

                    if count >= max_events:
                        break

                    if sleep_interval > 0:
                        time.sleep(sleep_interval)

                if count >= max_events or not loop:
                    break

                iteration += 1
                logger.info(f"Looping replay (iteration {iteration}) ...")

        except KeyboardInterrupt:
            print("\nStreaming interrupted by user.")
        finally:
            print("\nFlushing pending messages to Kafka broker ...")
            self.producer.flush()
            elapsed = time.time() - start_time
            actual_throughput = count / elapsed if elapsed > 0 else 0

            print("=" * 70)
            print("Kafka Replay Summary:")
            print(f"  - Total Events Emitted:       {self.total_sent:,}")
            print(f"  - Total Events Acknowledged:  {self.total_acknowledged:,}")
            print(f"  - Total Errors:               {self.total_errors}")
            print(f"  - Elapsed Time:               {elapsed:.2f}s")
            print(f"  - Mean Streaming Throughput:  {actual_throughput:.1f} events/sec")
            print("=" * 70)

        return count

    def close(self) -> None:
        """Close Kafka producer connection cleanly."""
        self.producer.flush()
        self.producer.close()


def find_default_dataset() -> Path:
    """Resolve default dataset path in order of preference."""
    candidates = [
        REPO_ROOT / "data" / "processed" / "aiops_kpi_clean.csv",
        REPO_ROOT / "data" / "sample" / "aiops_kpi_sample.csv",
        REPO_ROOT / "data" / "raw" / "phase2_train.csv",
    ]
    for c in candidates:
        if c.exists() and c.stat().st_size > 100:
            return c
    raise FileNotFoundError("No prepared dataset found. Run 'python scripts/prepare_aiops_kpi.py' first.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Kafka Streaming Replay Producer for AIOPS_KPI")
    parser.add_argument(
        "--dataset",
        type=Path,
        default=None,
        help="Path to CSV dataset (default: data/processed/aiops_kpi_clean.csv or sample)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="raw-metrics",
        help="Target Kafka topic (default: raw-metrics)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="Replay streaming rate in events/second (default: 10.0; 0 for max speed)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of events to emit (default: 100)",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        default=False,
        help="Continuously loop dataset (default: False)",
    )
    parser.add_argument(
        "--kpi-id",
        type=str,
        default=None,
        help="Filter replay for a specific KPI ID",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help="Kafka bootstrap server address (default from settings: localhost:9092)",
    )

    args = parser.parse_args()

    dataset_path = args.dataset or find_default_dataset()
    df = load_dataset_records(dataset_path, kpi_id=args.kpi_id, limit=args.limit)

    producer = StreamingReplayProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        rate_events_per_sec=args.rate,
    )

    try:
        producer.stream_dataframe(df, limit=args.limit, loop=args.loop)
    finally:
        producer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
