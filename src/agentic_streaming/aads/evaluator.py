"""Offline evaluation metrics and benchmark evaluator for AADS.

Computes precision, recall, F1-score, false positive rate, latency, and throughput
against historical benchmark ground truth labels without fabricating synthetic ground truth.
"""

import time
from typing import Any, Dict, List, Optional
import pandas as pd

from src.agentic_streaming.aads.detector import AADSDetector
from src.agentic_streaming.aads.config import AADSConfig
from src.schemas.evaluation import DetectionEvaluationMetrics


class AADSEvaluator:
    """Evaluates AADS performance on streaming time-series benchmark datasets."""

    def __init__(self, config: Optional[AADSConfig] = None) -> None:
        self.config = config or AADSConfig()
        self.detector = AADSDetector(self.config)

    def evaluate_dataframe(
        self,
        df: pd.DataFrame,
        value_col: str = "value",
        timestamp_col: str = "timestamp",
        label_col: str = "ground_truth",
        kpi_col: str = "kpi_id",
    ) -> Dict[str, Any]:
        """Evaluate AADS row-by-row on a pandas DataFrame."""
        self.detector.reset()

        tp = 0
        fp = 0
        tn = 0
        fn = 0
        latencies: List[float] = []

        start_time = time.time()

        for _, row in df.iterrows():
            record = {
                "value": float(row[value_col]),
                "timestamp": float(row[timestamp_col]),
                "metric_id": str(row.get(kpi_col, "default")),
            }
            res = self.detector.detect_one(record)
            latencies.append(res["processing_time_ms"])

            pred = res["is_anomaly"]
            actual = int(row[label_col]) if label_col in row and pd.notna(row[label_col]) else 0

            if pred and actual == 1:
                tp += 1
            elif pred and actual == 0:
                fp += 1
            elif not pred and actual == 0:
                tn += 1
            elif not pred and actual == 1:
                fn += 1

        total_elapsed = time.time() - start_time
        total_samples = len(df)
        throughput = total_samples / total_elapsed if total_elapsed > 0 else 0.0

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
        fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0
        mean_latency = sum(latencies) / len(latencies) if latencies else 0.0

        metrics_obj = DetectionEvaluationMetrics(
            detector_name=self.config.detector_name,
            precision=round(precision, 4),
            recall=round(recall, 4),
            f1_score=round(f1, 4),
            false_positive_rate=round(fpr, 4),
            total_samples=total_samples,
            true_positives=tp,
            false_positives=fp,
            true_negatives=tn,
            false_negatives=fn,
        )

        return {
            "metrics": metrics_obj,
            "summary": {
                "precision": round(precision, 4),
                "recall": round(recall, 4),
                "f1_score": round(f1, 4),
                "false_positive_rate": round(fpr, 4),
                "total_samples": total_samples,
                "anomalies_actual": tp + fn,
                "anomalies_detected": tp + fp,
                "true_positives": tp,
                "false_positives": fp,
                "true_negatives": tn,
                "false_negatives": fn,
                "mean_latency_ms": round(mean_latency, 4),
                "throughput_events_per_sec": round(throughput, 1),
                "elapsed_seconds": round(total_elapsed, 2),
            },
        }
