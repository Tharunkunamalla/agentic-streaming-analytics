"""Standalone Offline Evaluation and Validation Script for AADS Baseline.

Evaluates the 3-stage AADS streaming detector against real cloud benchmark data
(AIOPS_KPI) and ground truth labels before connecting to Kafka/Spark streaming.
"""

import argparse
import json
import sys
from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.agentic_streaming.aads.config import AADSConfig
from src.agentic_streaming.aads.evaluator import AADSEvaluator


def run_offline_evaluation(dataset_path: Path, limit: int | None = None) -> dict:
    """Run AADS row-by-row on historical benchmark data."""
    print("=" * 70)
    print("AADS Standalone Offline Benchmark Evaluation")
    print(f"Dataset: {dataset_path}")
    print("=" * 70)

    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found at {dataset_path}")

    df = pd.read_csv(dataset_path)
    if limit and limit > 0:
        df = df.iloc[:limit].copy()

    print(f"Loaded {len(df):,} benchmark rows.")
    print(f"Actual anomalies in evaluation subset: {int(df['ground_truth'].sum()):,}")

    config = AADSConfig(
        density_window_size=100,
        density_z_threshold=2.5,
        cluster_radius_factor=0.5,
        max_minor_cluster_size=3,
    )

    evaluator = AADSEvaluator(config)
    print("\nRunning online 3-stage detection row-by-row ...")
    results = evaluator.evaluate_dataframe(df)

    summary = results["summary"]
    print("=" * 70)
    print("AADS Offline Evaluation Results:")
    print("=" * 70)
    print(f"  - Precision:                 {summary['precision'] * 100:.2f}%")
    print(f"  - Recall:                    {summary['recall'] * 100:.2f}%")
    print(f"  - F1-Score:                  {summary['f1_score'] * 100:.2f}%")
    print(f"  - False Positive Rate (FPR): {summary['false_positive_rate'] * 100:.3f}%")
    print(f"  - True Positives (TP):       {summary['true_positives']:,}")
    print(f"  - False Positives (FP):      {summary['false_positives']:,}")
    print(f"  - True Negatives (TN):       {summary['true_negatives']:,}")
    print(f"  - False Negatives (FN):      {summary['false_negatives']:,}")
    print(f"  - Mean Detection Latency:    {summary['mean_latency_ms']:.4f} ms/event")
    print(f"  - Offline Throughput:        {summary['throughput_events_per_sec']:.1f} events/sec")
    print(f"  - Total Elapsed:             {summary['elapsed_seconds']:.2f}s")
    print("=" * 70)

    # Save evaluation output
    out_dir = REPO_ROOT / "experiments" / "baseline"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "aads_offline_evaluation.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"Results saved to: {out_file.relative_to(REPO_ROOT)}")

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="AADS Offline Benchmark Evaluation")
    parser.add_argument(
        "--dataset",
        type=Path,
        default=REPO_ROOT / "data" / "sample" / "aiops_kpi_sample.csv",
        help="Path to evaluation dataset CSV (default: data/sample/aiops_kpi_sample.csv)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit",
    )
    args = parser.parse_args()

    try:
        run_offline_evaluation(args.dataset, limit=args.limit)
        return 0
    except Exception as e:
        print(f"Error during AADS offline evaluation: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
