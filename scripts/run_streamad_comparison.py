"""StreamAD Official Comparison Benchmark Runner (Phase 6).

Runs:
1. AADS (Baseline Adapter)
2. xStream (Official StreamAD Adapter)
3. HSTree (Official StreamAD Adapter)
4. RRCF (Official StreamAD Adapter)

Evaluates all detectors on the exact same stream slice, produces the standardized
result schema, and prints/saves one standardized comparative evaluation table.
"""

import argparse
import json
from pathlib import Path
import sys
import time
import tracemalloc
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.agentic_streaming.detectors.interface import CommonStreamingDetector
from src.agentic_streaming.detectors.aads_adapter import AADSAdapter
from src.agentic_streaming.detectors.xstream_adapter import XStreamAdapter
from src.agentic_streaming.detectors.hstree_adapter import HSTreeAdapter
from src.agentic_streaming.detectors.rrcf_adapter import RRCFAdapter


def evaluate_detector(
    detector: CommonStreamingDetector,
    records: list[dict],
    ground_truths: list[int],
) -> dict:
    """Evaluate a detector on streaming records using the standardized interface."""
    detector.reset()
    latencies = []
    predictions = []
    scores = []

    tracemalloc.start()
    t_start = time.perf_counter()

    for rec in records:
        out = detector.process_record(rec)
        latencies.append(out["latency_ms"])
        predictions.append(1 if out["is_anomaly"] else 0)
        scores.append(out["anomaly_score"])

    total_time = time.perf_counter() - t_start
    _, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    n = len(records)
    tp = sum(1 for p, y in zip(predictions, ground_truths) if p == 1 and y == 1)
    fp = sum(1 for p, y in zip(predictions, ground_truths) if p == 1 and y == 0)
    tn = sum(1 for p, y in zip(predictions, ground_truths) if p == 0 and y == 0)
    fn = sum(1 for p, y in zip(predictions, ground_truths) if p == 0 and y == 1)

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0
    eps = n / total_time if total_time > 0 else 0.0
    mean_lat = float(np.mean(latencies)) if latencies else 0.0

    return {
        "detector": detector.name(),
        "total_records": n,
        "true_positives": tp,
        "false_positives": fp,
        "true_negatives": tn,
        "false_negatives": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
        "false_positive_rate": round(fpr, 5),
        "throughput_eps": round(eps, 1),
        "mean_latency_ms": round(mean_lat, 4),
        "peak_memory_mb": round(peak_mem / (1024 * 1024), 3),
        "elapsed_seconds": round(total_time, 3),
    }


def run_comparison_benchmark(dataset_path: Path, limit: int = 5000) -> list[dict]:
    """Execute standardized comparative benchmark."""
    print("=" * 95)
    print("STREAMAD OFFICIAL DETECTOR COMPARATIVE BENCHMARK (PHASE 6)")
    print(f"Dataset: {dataset_path} (evaluating first {limit:,} events)")
    print("=" * 95)

    df = pd.read_csv(dataset_path)
    if limit and limit > 0:
        df = df.iloc[:limit].copy()

    records = [
        {"value": float(row["value"]), "timestamp": float(row["timestamp"]), "event_id": f"evt-{i}"}
        for i, row in df.iterrows()
    ]
    ground_truths = df["ground_truth"].astype(int).tolist()
    actual_anomalies = sum(ground_truths)

    print(f"Loaded {len(records):,} streaming events ({actual_anomalies} actual anomalies).\n")

    detectors = [
        AADSAdapter(threshold=0.50),
        HSTreeAdapter(tree_height=8, tree_num=20, threshold=0.50),
        XStreamAdapter(n_components=15, n_chains=25, depth=8, window_len=100, threshold=0.50),
        RRCFAdapter(num_trees=15, tree_size=40, threshold=0.50),
    ]

    results = []
    for det in detectors:
        print(f"Running detector: {det.name():<12} ...", end="", flush=True)
        res = evaluate_detector(det, records, ground_truths)
        results.append(res)
        print(f" Done ({res['elapsed_seconds']:.2f}s | {res['throughput_eps']} eps)")

    # Print standardized comparison table
    print("\n" + "=" * 95)
    print(f"{'Detector':<12} | {'Precision':<9} | {'Recall':<8} | {'F1-Score':<8} | {'FPR':<8} | {'Throughput':<12} | {'Latency':<10} | {'Peak Mem'}")
    print("-" * 95)
    for r in results:
        print(
            f"{r['detector']:<12} | "
            f"{r['precision'] * 100:6.2f}%   | "
            f"{r['recall'] * 100:5.2f}%  | "
            f"{r['f1_score'] * 100:5.2f}%  | "
            f"{r['false_positive_rate'] * 100:5.3f}% | "
            f"{r['throughput_eps']:7.1f} eps   | "
            f"{r['mean_latency_ms']:6.4f} ms  | "
            f"{r['peak_memory_mb']:.2f} MB"
        )
    print("=" * 95)

    # Save results
    out_dir = REPO_ROOT / "experiments" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "streamad_comparison_results.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved comparison JSON: {json_path.relative_to(REPO_ROOT)}")

    csv_path = out_dir / "streamad_comparison_results.csv"
    pd.DataFrame(results).to_csv(csv_path, index=False)
    print(f"Saved comparison CSV:  {csv_path.relative_to(REPO_ROOT)}")

    # Generate comparative plot
    _plot_comparison(results, out_dir / "detector_comparison_metrics.png")

    return results


def _plot_comparison(results: list[dict], out_path: Path) -> None:
    """Generate comparative grouped bar chart."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5), dpi=300)

    names = [r["detector"] for r in results]
    x = np.arange(len(names))
    width = 0.22

    # Accuracy Metrics
    p_vals = [r["precision"] * 100 for r in results]
    r_vals = [r["recall"] * 100 for r in results]
    f1_vals = [r["f1_score"] * 100 for r in results]

    ax1.bar(x - width, p_vals, width, label="Precision", color="#3B82F6")
    ax1.bar(x, r_vals, width, label="Recall", color="#10B981")
    ax1.bar(x + width, f1_vals, width, label="F1-Score", color="#8B5CF6")
    ax1.set_xticks(x)
    ax1.set_xticklabels(names, fontweight="bold")
    ax1.set_ylabel("Percentage (%)", fontweight="bold")
    ax1.set_title("StreamAD Comparison: Detection Performance", fontweight="bold")
    ax1.legend(loc="upper right")
    ax1.grid(True, linestyle="--", alpha=0.5)

    # Operational Metrics: Latency & Throughput
    throughputs = [r["throughput_eps"] for r in results]
    colors = ["#2563EB", "#059669", "#D97706", "#DC2626"]
    bars2 = ax2.bar(names, throughputs, color=colors, width=0.5, edgecolor="#1E293B")
    ax2.set_ylabel("Throughput (events / sec)", fontweight="bold")
    ax2.set_title("Streaming Ingestion Throughput (EPS)", fontweight="bold")
    ax2.set_xticklabels(names, fontweight="bold")
    ax2.grid(True, linestyle="--", alpha=0.5)

    for bar, val in zip(bars2, throughputs):
        ax2.text(
            bar.get_x() + bar.get_width() / 2.0,
            bar.get_height() + 50,
            f"{val:.0f}",
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold",
        )

    plt.tight_layout()
    plt.savefig(out_path)
    plt.close(fig)
    print(f"Saved comparison plot: {out_path.relative_to(REPO_ROOT)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="StreamAD Comparison Benchmark")
    parser.add_argument(
        "--dataset",
        type=Path,
        default=REPO_ROOT / "data" / "sample" / "aiops_kpi_sample.csv",
        help="Evaluation dataset CSV path",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Number of streaming records to evaluate",
    )
    args = parser.parse_args()

    try:
        run_comparison_benchmark(args.dataset, limit=args.limit)
        return 0
    except Exception as e:
        print(f"Benchmark error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
