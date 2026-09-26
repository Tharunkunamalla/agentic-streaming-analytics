"""Benchmark and comparative evaluation of streaming anomaly detectors.

Compares:
1. AADS (Baseline)
2. HSTree (Half-Space Trees)
3. xStream (Multi-Projection Density)
4. RRCF (Robust Random Cut Forest)

Evaluates accuracy, false positive rate, throughput, and latency on the exact same stream slice.
"""

import argparse
import json
import sys
import time
from pathlib import Path
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.agentic_streaming.aads.config import AADSConfig
from src.agentic_streaming.aads.evaluator import AADSEvaluator
from src.agentic_streaming.detectors.hstree import HSTreeDetector
from src.agentic_streaming.detectors.xstream import XStreamDetector
from src.agentic_streaming.detectors.rrcf_detector import RRCFDetector


def evaluate_detector(detector_instance, values: list[float], ground_truths: list[int]) -> dict:
    """Evaluate detector row-by-row and measure latency and confusion matrix."""
    n = len(values)
    latencies = []
    tp = fp = tn = fn = 0

    start_time = time.perf_counter()
    for val, gt in zip(values, ground_truths):
        t0 = time.perf_counter()
        score, is_anom = detector_instance.process(val)
        latencies.append((time.perf_counter() - t0) * 1000.0)

        pred = 1 if is_anom else 0
        if pred == 1 and gt == 1:
            tp += 1
        elif pred == 1 and gt == 0:
            fp += 1
        elif pred == 0 and gt == 0:
            tn += 1
        else:
            fn += 1

    total_time = time.perf_counter() - start_time
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0
    eps = n / total_time if total_time > 0 else 0.0
    mean_lat = float(np.mean(latencies)) if latencies else 0.0

    return {
        "detector": detector_instance.name,
        "total_records": n,
        "elapsed_seconds": round(total_time, 3),
        "throughput_eps": round(eps, 1),
        "mean_latency_ms": round(mean_lat, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
        "false_positive_rate": round(fpr, 5),
        "true_positives": tp,
        "false_positives": fp,
        "true_negatives": tn,
        "false_negatives": fn,
    }


def run_benchmark(dataset_path: Path, limit: int | None = 5000) -> dict:
    """Run comparative benchmark across all 4 streaming detectors."""
    print("=" * 80)
    print("STREAMING ANOMALY DETECTOR COMPARATIVE BENCHMARK")
    print(f"Dataset: {dataset_path} (limit={limit})")
    print("=" * 80)

    df = pd.read_csv(dataset_path)
    if limit:
        df = df.iloc[:limit].copy()

    values = df["value"].astype(float).tolist()
    ground_truths = df["ground_truth"].astype(int).tolist()
    actual_anomalies = sum(ground_truths)

    print(f"Loaded {len(values):,} streaming records ({actual_anomalies} actual anomalies).\n")

    # 1. AADS Baseline
    print("Running AADS Baseline ...")
    aads_evaluator = AADSEvaluator(AADSConfig(density_window_size=100, density_z_threshold=2.5))
    aads_res = aads_evaluator.evaluate_dataframe(df)["summary"]
    aads_metric = {
        "detector": "AADS (Baseline)",
        "total_records": aads_res.get("total_samples", len(values)),
        "elapsed_seconds": aads_res["elapsed_seconds"],
        "throughput_eps": aads_res["throughput_events_per_sec"],
        "mean_latency_ms": aads_res["mean_latency_ms"],
        "precision": aads_res["precision"],
        "recall": aads_res["recall"],
        "f1_score": aads_res["f1_score"],
        "false_positive_rate": aads_res["false_positive_rate"],
        "true_positives": aads_res["true_positives"],
        "false_positives": aads_res["false_positives"],
        "true_negatives": aads_res["true_negatives"],
        "false_negatives": aads_res["false_negatives"],
    }

    # 2. HSTree
    print("Running Half-Space Trees (HSTree) ...")
    hstree_det = HSTreeDetector(n_trees=25, max_depth=8, window_size=150, threshold=0.90)
    hstree_metric = evaluate_detector(hstree_det, values, ground_truths)

    # 3. xStream
    print("Running xStream ...")
    xstream_det = XStreamDetector(n_chains=20, depth=5, window_size=200, threshold=0.65)
    xstream_metric = evaluate_detector(xstream_det, values, ground_truths)

    # 4. RRCF
    print("Running Robust Random Cut Forest (RRCF) ...")
    rrcf_det = RRCFDetector(num_trees=20, tree_size=100, threshold=0.95)
    rrcf_metric = evaluate_detector(rrcf_det, values, ground_truths)

    all_results = [aads_metric, hstree_metric, xstream_metric, rrcf_metric]

    # Print summary table
    print("\n" + "=" * 90)
    print(f"{'Detector':<18} | {'Precision':<9} | {'Recall':<8} | {'F1-Score':<8} | {'FPR':<8} | {'Throughput':<12} | {'Latency'}")
    print("-" * 90)
    for r in all_results:
        print(
            f"{r['detector']:<18} | "
            f"{r['precision'] * 100:6.2f}%   | "
            f"{r['recall'] * 100:5.2f}%  | "
            f"{r['f1_score'] * 100:5.2f}%  | "
            f"{r['false_positive_rate'] * 100:5.3f}% | "
            f"{r['throughput_eps']:7.1f} eps   | "
            f"{r['mean_latency_ms']:.4f} ms"
        )
    print("=" * 90)

    # Save output artifact
    out_dir = REPO_ROOT / "experiments" / "baseline"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "detector_comparison_benchmark.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nBenchmark report saved to: {out_path.relative_to(REPO_ROOT)}")

    return {"results": all_results}


def main() -> int:
    parser = argparse.ArgumentParser(description="Multi-Detector Comparative Benchmark")
    parser.add_argument(
        "--dataset",
        type=Path,
        default=REPO_ROOT / "data" / "sample" / "aiops_kpi_sample.csv",
        help="Path to evaluation dataset",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Number of records to evaluate",
    )
    args = parser.parse_args()

    try:
        run_benchmark(args.dataset, limit=args.limit)
        return 0
    except Exception as e:
        print(f"Error during benchmark: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
