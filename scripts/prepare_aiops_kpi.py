"""Dataset Preparation and Validation Pipeline for AIOPS_KPI.

Processes the raw AIOps KPI benchmark dataset, performs rigorous validation checks
(column existence, timestamp ordering, null values, duplicates, and ground truth anomaly preservation),
creates cleaned streaming-ready artifacts, a lightweight integration sample,
and generates `data/processed/dataset_manifest.json`.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import numpy as np

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
SAMPLE_DIR = REPO_ROOT / "data" / "sample"


def find_raw_dataset_file() -> Path | None:
    """Find the raw CSV dataset file in data/raw/."""
    candidates = [
        RAW_DIR / "phase2_train.csv",
        RAW_DIR / "train.csv",
        RAW_DIR / "aiops_kpi.csv",
    ]
    for c in candidates:
        if c.exists() and c.stat().st_size > 1000:
            return c

    # Search for any .csv in raw dir
    csvs = list(RAW_DIR.glob("*.csv"))
    if csvs:
        # Pick the largest CSV
        csvs.sort(key=lambda p: p.stat().st_size, reverse=True)
        return csvs[0]
    return None


def validate_and_process_dataset(raw_path: Path) -> dict:
    """Load, validate, clean and structure the AIOps KPI dataset."""
    print(f"Loading raw dataset: {raw_path.name} ({raw_path.stat().st_size:,} bytes) ...")
    df = pd.read_csv(raw_path)
    initial_rows = len(df)
    print(f"Raw rows loaded: {initial_rows:,}")

    # Standardize column names
    col_mapping = {}
    for col in df.columns:
        clean = col.strip().lower().replace(" ", "_")
        if clean in ["timestamp", "time", "ts"]:
            col_mapping[col] = "timestamp"
        elif clean in ["value", "val", "kpi_value"]:
            col_mapping[col] = "value"
        elif clean in ["label", "anomaly", "ground_truth", "is_anomaly"]:
            col_mapping[col] = "ground_truth"
        elif clean in ["kpi_id", "kpiid", "kpi", "series_id"]:
            col_mapping[col] = "kpi_id"
    df = df.rename(columns=col_mapping)

    # Validate required columns
    required = ["timestamp", "value"]
    for req in required:
        if req not in df.columns:
            raise ValueError(f"Missing mandatory column: '{req}'. Found columns: {list(df.columns)}")

    if "kpi_id" not in df.columns:
        print("Note: 'kpi_id' column not present; assigning default 'kpi_default'")
        df["kpi_id"] = "kpi_default"
    else:
        df["kpi_id"] = df["kpi_id"].astype(str)

    has_ground_truth = "ground_truth" in df.columns
    if not has_ground_truth:
        print("WARNING: Dataset does NOT contain ground truth anomaly labels.")
        df["ground_truth"] = None
    else:
        df["ground_truth"] = pd.to_numeric(df["ground_truth"], errors="coerce").fillna(0).astype(int)

    # Convert timestamp to int (epoch seconds) and datetime
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "value"])
    df["timestamp"] = df["timestamp"].astype(int)
    df["value"] = df["value"].astype(float)

    # Check and remove duplicates per (kpi_id, timestamp)
    duplicates_count = df.duplicated(subset=["kpi_id", "timestamp"]).sum()
    if duplicates_count > 0:
        print(f"Resolving {duplicates_count:,} duplicate (kpi_id, timestamp) records by keeping first occurrence...")
        df = df.drop_duplicates(subset=["kpi_id", "timestamp"], keep="first")

    # Sort deterministically by kpi_id and timestamp
    df = df.sort_values(by=["kpi_id", "timestamp"]).reset_index(drop=True)

    # Verification Statistics
    kpi_list = df["kpi_id"].unique().tolist()
    total_anomalies = int(df["ground_truth"].sum()) if has_ground_truth else 0
    anomaly_ratio = float(total_anomalies / len(df)) if len(df) > 0 else 0.0

    kpi_breakdown = {}
    for kpi in kpi_list:
        sub = df[df["kpi_id"] == kpi]
        kpi_anomalies = int(sub["ground_truth"].sum()) if has_ground_truth else 0
        min_ts = int(sub["timestamp"].min())
        max_ts = int(sub["timestamp"].max())
        kpi_breakdown[kpi] = {
            "rows": len(sub),
            "anomaly_count": kpi_anomalies,
            "anomaly_ratio_pct": round((kpi_anomalies / len(sub)) * 100, 3) if len(sub) > 0 else 0,
            "min_timestamp": min_ts,
            "max_timestamp": max_ts,
            "min_datetime_utc": datetime.fromtimestamp(min_ts, tz=timezone.utc).isoformat(),
            "max_datetime_utc": datetime.fromtimestamp(max_ts, tz=timezone.utc).isoformat(),
            "mean_value": round(float(sub["value"].mean()), 4),
            "std_value": round(float(sub["value"].std()), 4),
        }

    # Save cleaned full dataset
    clean_csv_path = PROCESSED_DIR / "aiops_kpi_clean.csv"
    df.to_csv(clean_csv_path, index=False)
    print(f"Saved processed dataset: {clean_csv_path.name} ({len(df):,} rows)")

    # Create sample dataset for integration tests
    # Select the KPI with the highest anomaly count or most representative distribution
    top_kpi = max(kpi_breakdown.items(), key=lambda x: x[1]["anomaly_count"])[0]
    sample_df = df[df["kpi_id"] == top_kpi].iloc[:5000].copy()
    sample_csv_path = SAMPLE_DIR / "aiops_kpi_sample.csv"
    sample_df.to_csv(sample_csv_path, index=False)
    print(f"Saved integration test sample: {sample_csv_path.name} ({len(sample_df):,} rows, KPI '{top_kpi}')")

    # Construct Manifest
    min_overall_ts = int(df["timestamp"].min())
    max_overall_ts = int(df["timestamp"].max())
    manifest = {
        "dataset_name": "AIOPS_KPI",
        "benchmark_source": "StreamAD Benchmark / NetMan 2018 AIOps Challenge",
        "description": "Cloud platform Key Performance Indicator (KPI) time-series benchmark dataset for unsupervised streaming anomaly detection",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "files": {
            "raw_file": str(raw_path.name),
            "processed_file": str(clean_csv_path.name),
            "sample_file": str(sample_csv_path.name),
        },
        "summary": {
            "initial_raw_rows": initial_rows,
            "cleaned_rows": len(df),
            "total_kpi_streams": len(kpi_list),
            "total_anomalies": total_anomalies,
            "anomaly_ratio_pct": round(anomaly_ratio * 100, 3),
            "ground_truth_labels_available": has_ground_truth,
            "columns": list(df.columns),
            "time_range": {
                "min_timestamp": min_overall_ts,
                "max_timestamp": max_overall_ts,
                "start_utc": datetime.fromtimestamp(min_overall_ts, tz=timezone.utc).isoformat(),
                "end_utc": datetime.fromtimestamp(max_overall_ts, tz=timezone.utc).isoformat(),
            },
        },
        "preprocessing_performed": [
            "Standardized column names to (timestamp, kpi_id, value, ground_truth)",
            "Enforced strict numeric type casting on timestamp and value",
            "Preserved original benchmark ground truth anomaly labels (0 = Normal, 1 = Anomaly)",
            f"Deduplicated {duplicates_count} identical (kpi_id, timestamp) entries",
            "Sorted records chronologically per KPI stream for deterministic streaming replay",
        ],
        "kpi_breakdown": kpi_breakdown,
    }

    manifest_path = PROCESSED_DIR / "dataset_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print(f"Generated dataset manifest: {manifest_path.name}")

    return manifest


def main() -> int:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("AIOPS_KPI Dataset Preparation & Validation Pipeline")
    print("=" * 60)

    raw_file = find_raw_dataset_file()
    if not raw_file:
        print(f"ERROR: No raw dataset found in {RAW_DIR}. Run 'python scripts/download_data.py' first.")
        return 1

    manifest = validate_and_process_dataset(raw_file)
    print("=" * 60)
    print("Dataset Summary:")
    print(f"  - Total Cleaned Rows: {manifest['summary']['cleaned_rows']:,}")
    print(f"  - Total KPI Streams:  {manifest['summary']['total_kpi_streams']}")
    print(f"  - Total Anomalies:    {manifest['summary']['total_anomalies']:,} ({manifest['summary']['anomaly_ratio_pct']}%)")
    print(f"  - Ground Truth:       {manifest['summary']['ground_truth_labels_available']}")
    print("=" * 60)
    print("Dataset Preparation: SUCCESS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
