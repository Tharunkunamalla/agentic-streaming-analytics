"""Utility to programmatically create and verify all required Kafka streaming topics."""

import sys
import time
from pathlib import Path
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.config.settings import get_settings
from src.utils.logger import get_logger

logger = get_logger("kafka_topic_init")

REQUIRED_TOPICS = [
    "raw-metrics",
    "processed-metrics",
    "anomaly-events",
    "agent-decisions",
    "analytics-results",
]


def init_topics(bootstrap_servers: str = "localhost:9092", partitions: int = 3, replication_factor: int = 1) -> bool:
    """Create all required Kafka topics if they do not exist."""
    print(f"Connecting to Kafka broker at {bootstrap_servers} ...")
    max_retries = 10
    admin = None
    for attempt in range(1, max_retries + 1):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id="topic_initializer",
                request_timeout_ms=10000,
            )
            print("Successfully connected to Kafka broker.")
            break
        except NoBrokersAvailable:
            print(f"Broker not ready yet (attempt {attempt}/{max_retries}). Retrying in 2s...")
            time.sleep(2)

    if not admin:
        print("ERROR: Could not connect to Kafka broker. Ensure docker-compose is up.")
        return False

    try:
        existing_topics = set(admin.list_topics())
        print(f"Existing topics in cluster: {sorted(list(existing_topics))}")

        new_topics = []
        for topic in REQUIRED_TOPICS:
            if topic not in existing_topics:
                new_topics.append(
                    NewTopic(
                        name=topic,
                        num_partitions=partitions,
                        replication_factor=replication_factor,
                    )
                )

        if new_topics:
            print(f"Creating {len(new_topics)} new topic(s): {[t.name for t in new_topics]}")
            admin.create_topics(new_topics=new_topics, validate_only=False)
            print("Topic creation request submitted successfully.")
        else:
            print("All required streaming topics already exist.")

        # Re-verify
        time.sleep(1)
        final_topics = set(admin.list_topics())
        print("=" * 60)
        print("Kafka Topics Status:")
        for topic in REQUIRED_TOPICS:
            status = "EXISTS" if topic in final_topics else "MISSING"
            print(f"  - {topic}: {status}")
        print("=" * 60)
        return all(t in final_topics for t in REQUIRED_TOPICS)
    except Exception as e:
        print(f"Error during topic management: {e}")
        return False
    finally:
        if admin:
            admin.close()


def main() -> int:
    settings = get_settings()
    success = init_topics(bootstrap_servers=settings.kafka_bootstrap_servers)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
