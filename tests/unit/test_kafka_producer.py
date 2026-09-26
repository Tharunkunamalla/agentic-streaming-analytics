"""Unit tests for Kafka replay producer logic, dataset filtering, and serialization."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from src.agentic_streaming.kafka.producer import (
    StreamingReplayProducer,
    load_dataset_records,
)
from src.schemas.metric import MetricRecord

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SAMPLE_CSV = REPO_ROOT / "data" / "sample" / "aiops_kpi_sample.csv"


def test_load_dataset_records_limit():
    """Verify loading dataset records with limit parameter."""
    df = load_dataset_records(SAMPLE_CSV, limit=50)
    assert len(df) == 50
    assert "timestamp" in df.columns
    assert "value" in df.columns
    assert "ground_truth" in df.columns


def test_load_dataset_records_filter_kpi():
    """Verify loading and filtering dataset by KPI ID."""
    df_raw = pd.read_csv(SAMPLE_CSV, nrows=10)
    kpi_id = df_raw["kpi_id"].iloc[0]

    df_filtered = load_dataset_records(SAMPLE_CSV, kpi_id=kpi_id, limit=20)
    assert len(df_filtered) == 20
    assert (df_filtered["kpi_id"] == kpi_id).all()


@patch("src.agentic_streaming.kafka.producer.KafkaProducer")
def test_producer_send_record(mock_kafka_class):
    """Verify send_record publishes properly formatted MetricRecord to Kafka topic."""
    mock_instance = MagicMock()
    mock_future = MagicMock()
    mock_instance.send.return_value = mock_future
    mock_kafka_class.return_value = mock_instance

    producer = StreamingReplayProducer(
        bootstrap_servers="localhost:9092",
        topic="raw-metrics",
        rate_events_per_sec=0,
    )

    record = MetricRecord(
        timestamp=1493568000.0,
        metric_id="kpi-test-123",
        value=42.5,
        ground_truth=1,
    )

    producer.send_record(record)

    assert mock_instance.send.called
    call_args = mock_instance.send.call_args
    assert call_args[0][0] == "raw-metrics"
    assert call_args[1]["key"] == "kpi-test-123"
    assert call_args[1]["value"]["value"] == 42.5
    assert call_args[1]["value"]["ground_truth"] == 1
    assert "event_id" in call_args[1]["value"]
    assert "ingestion_timestamp" in call_args[1]["value"]


@patch("src.agentic_streaming.kafka.producer.KafkaProducer")
def test_stream_dataframe_execution(mock_kafka_class):
    """Verify stream_dataframe replays exact number of requested limit rows."""
    mock_instance = MagicMock()
    mock_future = MagicMock()
    mock_instance.send.return_value = mock_future
    mock_kafka_class.return_value = mock_instance

    df = pd.DataFrame([
        {"timestamp": 1000 + i, "value": 10.0 + i, "ground_truth": 0, "kpi_id": "kpi_01"}
        for i in range(15)
    ])

    producer = StreamingReplayProducer(
        bootstrap_servers="localhost:9092",
        topic="raw-metrics",
        rate_events_per_sec=0,
    )

    count = producer.stream_dataframe(df, limit=10, loop=False)

    assert count == 10
    assert producer.total_sent == 10
    assert mock_instance.send.call_count == 10
    assert mock_instance.flush.called
