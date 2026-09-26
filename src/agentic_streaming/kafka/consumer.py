"""Kafka Consumer Utility for validating and monitoring raw-metrics stream."""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Generator
from kafka import KafkaConsumer
from kafka.errors import KafkaError

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.config.settings import get_settings
from src.schemas.metric import MetricRecord
from src.utils.logger import get_logger

logger = get_logger("kafka_consumer")


class StreamingConsumer:
    """Kafka consumer that reads and validates time-series events from a topic."""

    def __init__(
        self,
        topic: str = "raw-metrics",
        bootstrap_servers: str | None = None,
        group_id: str = "validation-consumer-group",
        auto_offset_reset: str = "earliest",
        timeout_ms: int = 10000,
    ) -> None:
        settings = get_settings()
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=timeout_ms,
        )

    def consume_records(self, max_records: int | None = None) -> Generator[MetricRecord, None, None]:
        """Consume and yield validated MetricRecord objects."""
        count = 0
        for msg in self.consumer:
            data = msg.value
            record = MetricRecord.model_validate(data)
            count += 1
            yield record
            if max_records and count >= max_records:
                break

    def close(self) -> None:
        """Close Kafka consumer connection."""
        self.consumer.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Kafka Metric Consumer & Validator")
    parser.add_argument("--topic", type=str, default="raw-metrics")
    parser.add_argument("--max", type=int, default=10, help="Max records to read")
    parser.add_argument("--bootstrap-servers", type=str, default=None)
    args = parser.parse_args()

    print(f"Listening for up to {args.max} messages on topic '{args.topic}' ...")
    consumer = StreamingConsumer(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="earliest",
    )

    count = 0
    anomalies = 0
    try:
        for record in consumer.consume_records(max_records=args.max):
            count += 1
            if record.ground_truth == 1:
                anomalies += 1
            print(
                f"[{count}] event_id={record.event_id[:8]}... | ts={int(record.timestamp)} | "
                f"kpi={record.metric_id[:8]}... | val={record.value:.4f} | label={record.ground_truth}"
            )
        print(f"\nConsumed {count} records ({anomalies} anomalies). Schema validation: SUCCESS")
        return 0
    except Exception as e:
        print(f"Consumer error: {e}")
        return 1
    finally:
        consumer.close()


if __name__ == "__main__":
    sys.exit(main())
