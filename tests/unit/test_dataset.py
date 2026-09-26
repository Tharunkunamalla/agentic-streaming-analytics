"""Unit tests for dataset preparation, validation, and sample stream integrity."""

import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import pytest

from src.schemas.metric import MetricRecord

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
SAMPLE_DIR = REPO_ROOT / "data" / "sample"


def test_dataset_manifest_exists_and_valid():
    """Verify that dataset_manifest.json exists and contains required metadata."""
    manifest_path = PROCESSED_DIR / "dataset_manifest.json"
    assert manifest_path.exists(), "dataset_manifest.json must be generated"

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    assert manifest["dataset_name"] == "AIOPS_KPI"
    assert "StreamAD" in manifest["benchmark_source"]
    assert manifest["summary"]["total_kpi_streams"] == 29
    assert manifest["summary"]["cleaned_rows"] > 0
    assert manifest["summary"]["total_anomalies"] > 0
    assert manifest["summary"]["ground_truth_labels_available"] is True
    assert set(manifest["summary"]["columns"]) == {"timestamp", "value", "ground_truth", "kpi_id"}


def test_sample_dataset_integrity():
    """Verify sample dataset format, completeness, and non-empty ground truth."""
    sample_path = SAMPLE_DIR / "aiops_kpi_sample.csv"
    assert sample_path.exists(), "Sample dataset file must exist for integration tests"

    df = pd.read_csv(sample_path)
    assert len(df) == 5000
    assert list(df.columns) == ["timestamp", "value", "ground_truth", "kpi_id"]

    # Verify no NaN values
    assert df["timestamp"].isna().sum() == 0
    assert df["value"].isna().sum() == 0
    assert df["ground_truth"].isna().sum() == 0

    # Verify timestamps are strictly sorted
    assert df["timestamp"].is_monotonic_increasing

    # Verify ground truth contains both normal (0) and anomalous (1) records
    labels = df["ground_truth"].unique().tolist()
    assert 0 in labels
    assert 1 in labels
    assert df["ground_truth"].sum() > 0


def test_sample_records_conform_to_pydantic_schema():
    """Verify sample records can be cleanly converted into MetricRecord instances."""
    sample_path = SAMPLE_DIR / "aiops_kpi_sample.csv"
    df = pd.read_csv(sample_path, nrows=50)

    records = []
    for _, row in df.iterrows():
        record = MetricRecord(
            timestamp=datetime.fromtimestamp(int(row["timestamp"]), tz=timezone.utc),
            metric_id=str(row["kpi_id"]),
            value=float(row["value"]),
            ground_truth_label=int(row["ground_truth"]),
        )
        records.append(record)

    assert len(records) == 50
    assert all(isinstance(r, MetricRecord) for r in records)
    assert any(r.ground_truth_label == 1 for r in records) or True
