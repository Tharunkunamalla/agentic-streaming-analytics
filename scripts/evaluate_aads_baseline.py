"""Phase 5: Evaluation Framework for AADS Baseline.

Computes:
- Precision, Recall, F1, False Positive Rate (FPR)
- Detection Latency (mean, p50, p95, p99)
- Throughput (events/sec)
- Memory usage (peak and delta via tracemalloc)

Generates:
- experiments/results/aads_baseline_metrics.json
- experiments/results/aads_baseline_metrics.csv
- experiments/results/precision_recall_f1.png
- experiments/results/latency.png
- experiments/results/throughput.png
- experiments/results/anomaly_timeline.png
"""

import argparse
import json
from pathlib import Path
import sys
import time
import tracemalloc
import matplotlib
matplotlib.use("Agg")  # Non-interactive headless backend
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.agentic_streaming.aads.config import AADSConfig
from src.agentic_streaming.aads.detector import AADSDetector


def run_evaluation(config_path: Path) -> dict:
    """Run AADS baseline evaluation from a configuration file."""
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # Resolve dataset path
    data_path = REPO_ROOT / cfg["dataset"].get("path", "data/sample/aiops_kpi_sample.csv")
    if not data_path.exists():
        data_path = REPO_ROOT / cfg["dataset"].get("full_path", "data/processed/aiops_kpi_clean.csv")

    print("=" * 70)
    print("AADS Baseline Evaluation Framework (Phase 5)")
    print(f"Config:  {config_path.relative_to(REPO_ROOT)}")
    print(f"Dataset: {data_path.relative_to(REPO_ROOT)}")
    print("=" * 70)

    df = pd.read_csv(data_path)
    limit = cfg["dataset"].get("limit", None)
    if limit and limit > 0:
        df = df.iloc[:limit].copy()

    val_col = cfg["dataset"].get("value_col", "value")
    label_col = cfg["dataset"].get("label_col", "ground_truth")
    ts_col = cfg["dataset"].get("time_col", "timestamp")

    values = df[val_col].astype(float).values
    labels = df[label_col].astype(int).values
    timestamps = df[ts_col].values
    n = len(values)
    actual_anomalies = int(labels.sum())

    print(f"Processing {n:,} stream events ({actual_anomalies:,} actual anomalies) ...")

    # Initialize AADS detector from configuration
    det_cfg = cfg["detector"]
    aads_config = AADSConfig(
        density_window_size=det_cfg.get("density_window_size", 100),
        density_z_threshold=det_cfg.get("density_z_threshold", 2.5),
        density_decay_alpha=det_cfg.get("density_decay_alpha", 0.05),
        cluster_radius_factor=det_cfg.get("cluster_radius_factor", 0.5),
        max_minor_cluster_size=det_cfg.get("max_minor_cluster_size", 3),
        cluster_support_ratio_threshold=det_cfg.get("cluster_support_ratio_threshold", 0.05),
        cluster_max_idle_steps=det_cfg.get("cluster_max_idle_steps", 500),
    )
    detector = AADSDetector(aads_config)

    # Start memory and latency profiling
    tracemalloc.start()
    latencies_ms = []
    predictions = []
    scores = []

    start_wall_time = time.perf_counter()
    for idx in range(n):
        t0 = time.perf_counter()
        res = detector.process_event(
            event_id=f"evt-{idx}",
            timestamp=float(timestamps[idx]),
            value=float(values[idx]),
            metric_id="kpi-stream",
        )
        lat = (time.perf_counter() - t0) * 1000.0  # ms
        latencies_ms.append(lat)
        predictions.append(1 if res["is_anomaly"] else 0)
        scores.append(res["anomaly_score"] or 0.0)

    total_wall_time = time.perf_counter() - start_wall_time
    current_mem, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # Calculate confusion matrix & metrics
    tp = sum(1 for p, y in zip(predictions, labels) if p == 1 and y == 1)
    fp = sum(1 for p, y in zip(predictions, labels) if p == 1 and y == 0)
    tn = sum(1 for p, y in zip(predictions, labels) if p == 0 and y == 0)
    fn = sum(1 for p, y in zip(predictions, labels) if p == 0 and y == 1)

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0
    throughput = n / total_wall_time if total_wall_time > 0 else 0.0

    lat_arr = np.array(latencies_ms)
    metrics = {
        "experiment_name": cfg.get("experiment_name", "aads_baseline"),
        "total_records": n,
        "actual_anomalies": actual_anomalies,
        "predicted_anomalies": int(sum(predictions)),
        "true_positives": tp,
        "false_positives": fp,
        "true_negatives": tn,
        "false_negatives": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
        "false_positive_rate": round(fpr, 5),
        "throughput_events_per_sec": round(throughput, 1),
        "total_elapsed_seconds": round(total_wall_time, 3),
        "latency_ms": {
            "mean": round(float(np.mean(lat_arr)), 4),
            "median": round(float(np.median(lat_arr)), 4),
            "p95": round(float(np.percentile(lat_arr, 95)), 4),
            "p99": round(float(np.percentile(lat_arr, 99)), 4),
            "min": round(float(np.min(lat_arr)), 4),
            "max": round(float(np.max(lat_arr)), 4),
        },
        "memory_usage": {
            "current_mb": round(current_mem / (1024 * 1024), 3),
            "peak_mb": round(peak_mem / (1024 * 1024), 3),
        },
        "configuration": cfg,
    }

    # Print summary
    print("\n" + "=" * 70)
    print("AADS Baseline Evaluation Results:")
    print("=" * 70)
    print(f"  - Precision:                 {precision * 100:.2f}%")
    print(f"  - Recall:                    {recall * 100:.2f}%")
    print(f"  - F1-Score:                  {f1 * 100:.2f}%")
    print(f"  - False Positive Rate (FPR): {fpr * 100:.3f}%")
    print(f"  - Throughput:                {throughput:.1f} events/sec")
    print(f"  - Mean Latency:              {metrics['latency_ms']['mean']:.4f} ms")
    print(f"  - 95th Percentile Latency:   {metrics['latency_ms']['p95']:.4f} ms")
    print(f"  - Peak Memory Usage:         {metrics['memory_usage']['peak_mb']:.3f} MB")
    print("=" * 70)

    # Ensure output directories exist
    results_dir = REPO_ROOT / cfg["output"].get("results_dir", "experiments/results")
    results_dir.mkdir(parents=True, exist_ok=True)

    # 1. Save JSON
    json_path = REPO_ROOT / cfg["output"].get("metrics_json", "experiments/results/aads_baseline_metrics.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    print(f"Metrics saved: {json_path.relative_to(REPO_ROOT)}")

    # 2. Save CSV
    csv_path = REPO_ROOT / cfg["output"].get("metrics_csv", "experiments/results/aads_baseline_metrics.csv")
    flat_row = {
        "experiment_name": metrics["experiment_name"],
        "total_records": metrics["total_records"],
        "actual_anomalies": metrics["actual_anomalies"],
        "predicted_anomalies": metrics["predicted_anomalies"],
        "precision": metrics["precision"],
        "recall": metrics["recall"],
        "f1_score": metrics["f1_score"],
        "false_positive_rate": metrics["false_positive_rate"],
        "throughput_eps": metrics["throughput_events_per_sec"],
        "latency_mean_ms": metrics["latency_ms"]["mean"],
        "latency_p95_ms": metrics["latency_ms"]["p95"],
        "latency_p99_ms": metrics["latency_ms"]["p99"],
        "peak_memory_mb": metrics["memory_usage"]["peak_mb"],
    }
    pd.DataFrame([flat_row]).to_csv(csv_path, index=False)
    print(f"CSV saved:     {csv_path.relative_to(REPO_ROOT)}")

    # 3. Generate plots
    _generate_plots(
        results_dir=results_dir,
        metrics=metrics,
        latencies_ms=latencies_ms,
        timestamps=timestamps,
        values=values,
        labels=labels,
        predictions=predictions,
    )

    return metrics


def _generate_plots(
    results_dir: Path,
    metrics: dict,
    latencies_ms: list[float],
    timestamps: np.ndarray,
    values: np.ndarray,
    labels: np.ndarray,
    predictions: list[int],
) -> None:
    """Generate high-quality academic visualization figures."""
    # Plot style setup
    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
    plt.rcParams["font.sans-serif"] = "DejaVu Sans"

    # 1. precision_recall_f1.png
    fig, ax = plt.subplots(figsize=(8, 5), dpi=300)
    chart_metrics = ["Precision", "Recall", "F1-Score", "FPR"]
    chart_vals = [
        metrics["precision"] * 100,
        metrics["recall"] * 100,
        metrics["f1_score"] * 100,
        metrics["false_positive_rate"] * 100,
    ]
    colors = ["#2563EB", "#059669", "#7C3AED", "#DC2626"]
    bars = ax.bar(chart_metrics, chart_vals, color=colors, width=0.55, edgecolor="#1E293B", linewidth=1.2)
    ax.set_ylabel("Percentage (%)", fontsize=11, fontweight="bold")
    ax.set_title("AADS Streaming Baseline: Accuracy & Error Metrics", fontsize=13, fontweight="bold", pad=12)
    ax.set_ylim(0, max(max(chart_vals) * 1.25, 30.0))

    for bar, val in zip(bars, chart_vals):
        yval = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            yval + 0.8,
            f"{val:.2f}%",
            ha="center",
            va="bottom",
            fontsize=10,
            fontweight="bold",
        )

    plt.tight_layout()
    p1 = results_dir / "precision_recall_f1.png"
    plt.savefig(p1)
    plt.close(fig)
    print(f"Plot saved:    {p1.relative_to(REPO_ROOT)}")

    # 2. latency.png
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5), dpi=300)
    lat_arr = np.array(latencies_ms)
    p99 = np.percentile(lat_arr, 99)
    filtered_lat = lat_arr[lat_arr <= p99 * 1.5]

    ax1.hist(filtered_lat, bins=40, color="#3B82F6", edgecolor="#1D4ED8", alpha=0.75)
    ax1.axvline(metrics["latency_ms"]["mean"], color="#EF4444", linestyle="--", linewidth=1.8, label=f"Mean: {metrics['latency_ms']['mean']:.4f} ms")
    ax1.axvline(metrics["latency_ms"]["p95"], color="#F59E0B", linestyle=":", linewidth=1.8, label=f"P95: {metrics['latency_ms']['p95']:.4f} ms")
    ax1.set_xlabel("Latency (ms / event)", fontsize=10, fontweight="bold")
    ax1.set_ylabel("Frequency", fontsize=10, fontweight="bold")
    ax1.set_title("Event Classification Latency Distribution", fontsize=11, fontweight="bold")
    ax1.legend(loc="upper right", frameon=True)

    # CDF
    sorted_lat = np.sort(lat_arr)
    cdf = np.arange(1, len(sorted_lat) + 1) / len(sorted_lat)
    ax2.plot(sorted_lat, cdf, color="#059669", linewidth=2.0)
    ax2.set_xlim(0, p99 * 1.5)
    ax2.set_xlabel("Latency (ms / event)", fontsize=10, fontweight="bold")
    ax2.set_ylabel("Cumulative Probability", fontsize=10, fontweight="bold")
    ax2.set_title("Latency Empirical CDF", fontsize=11, fontweight="bold")
    ax2.grid(True, linestyle="--", alpha=0.6)

    plt.tight_layout()
    p2 = results_dir / "latency.png"
    plt.savefig(p2)
    plt.close(fig)
    print(f"Plot saved:    {p2.relative_to(REPO_ROOT)}")

    # 3. throughput.png
    fig, ax = plt.subplots(figsize=(10, 4.5), dpi=300)
    chunk_size = 250
    num_chunks = len(latencies_ms) // chunk_size
    chunk_throughputs = []
    chunk_indices = []

    for i in range(num_chunks):
        chunk_lats = latencies_ms[i * chunk_size : (i + 1) * chunk_size]
        chunk_time_sec = sum(chunk_lats) / 1000.0
        if chunk_time_sec > 0:
            chunk_throughputs.append(chunk_size / chunk_time_sec)
            chunk_indices.append((i + 1) * chunk_size)

    ax.plot(chunk_indices, chunk_throughputs, color="#7C3AED", linewidth=1.8, marker="o", markersize=4, label="Rolling Chunk Throughput")
    ax.axhline(metrics["throughput_events_per_sec"], color="#DC2626", linestyle="--", linewidth=1.8, label=f"Average Throughput: {metrics['throughput_events_per_sec']:.1f} eps")
    ax.set_xlabel("Processed Stream Events", fontsize=10, fontweight="bold")
    ax.set_ylabel("Throughput (events / sec)", fontsize=10, fontweight="bold")
    ax.set_title("Streaming Ingestion & Classification Throughput", fontsize=12, fontweight="bold", pad=10)
    ax.legend(loc="lower right", frameon=True)

    plt.tight_layout()
    p3 = results_dir / "throughput.png"
    plt.savefig(p3)
    plt.close(fig)
    print(f"Plot saved:    {p3.relative_to(REPO_ROOT)}")

    # 4. anomaly_timeline.png
    fig, ax = plt.subplots(figsize=(14, 5), dpi=300)
    # Take representative slice of 1,000 points to keep visualization crisp
    plot_slice = min(len(values), 1200)
    x_axis = np.arange(plot_slice)
    sub_vals = values[:plot_slice]
    sub_gt = labels[:plot_slice]
    sub_pred = np.array(predictions[:plot_slice])

    ax.plot(x_axis, sub_vals, color="#64748B", linewidth=1.2, alpha=0.85, label="KPI Metric Stream")

    # True Positives (pred=1, gt=1)
    tp_idx = np.where((sub_pred == 1) & (sub_gt == 1))[0]
    if len(tp_idx) > 0:
        ax.scatter(tp_idx, sub_vals[tp_idx], color="#059669", s=45, zorder=5, marker="^", label="True Positive (TP)")

    # False Positives (pred=1, gt=0)
    fp_idx = np.where((sub_pred == 1) & (sub_gt == 0))[0]
    if len(fp_idx) > 0:
        ax.scatter(fp_idx, sub_vals[fp_idx], color="#EF4444", s=35, zorder=4, marker="x", label="False Positive (FP)")

    # False Negatives (pred=0, gt=1)
    fn_idx = np.where((sub_pred == 0) & (sub_gt == 1))[0]
    if len(fn_idx) > 0:
        ax.scatter(fn_idx, sub_vals[fn_idx], color="#F59E0B", s=45, zorder=4, marker="o", facecolors="none", edgecolors="#F59E0B", linewidth=1.5, label="Missed Anomaly (FN)")

    ax.set_xlabel("Stream Sequence Index (Event Order)", fontsize=10, fontweight="bold")
    ax.set_ylabel("Observed Metric Value", fontsize=10, fontweight="bold")
    ax.set_title("AADS Real-Time Stream Detection Timeline vs Ground Truth", fontsize=12, fontweight="bold", pad=10)
    ax.legend(loc="upper right", frameon=True, fontsize=9)

    plt.tight_layout()
    p4 = results_dir / "anomaly_timeline.png"
    plt.savefig(p4)
    plt.close(fig)
    print(f"Plot saved:    {p4.relative_to(REPO_ROOT)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate AADS Baseline Framework")
    parser.add_argument(
        "--config",
        type=Path,
        default=REPO_ROOT / "config" / "experiments" / "aads_baseline.yaml",
        help="Path to YAML configuration file",
    )
    args = parser.parse_args()

    try:
        run_evaluation(args.config)
        return 0
    except Exception as e:
        print(f"Evaluation error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
