"""Unit tests for AADSEvaluator offline benchmarking metrics."""

import pandas as pd
import pytest
from src.agentic_streaming.aads.config import AADSConfig
from src.agentic_streaming.aads.evaluator import AADSEvaluator
from src.schemas.evaluation import DetectionEvaluationMetrics


def test_evaluator_dataframe_computation():
    """Verify evaluator produces complete DetectionEvaluationMetrics on test dataframe."""
    config = AADSConfig(min_warmup_samples=10, density_z_threshold=2.5)
    evaluator = AADSEvaluator(config)

    rows = []
    # 40 nominal points
    for i in range(40):
        rows.append({"timestamp": 1000 + i, "value": 20.0, "ground_truth": 0, "kpi_id": "test_kpi"})

    # 2 true anomaly spikes
    rows.append({"timestamp": 1041, "value": 99.0, "ground_truth": 1, "kpi_id": "test_kpi"})
    rows.append({"timestamp": 1042, "value": 105.0, "ground_truth": 1, "kpi_id": "test_kpi"})

    df = pd.DataFrame(rows)
    eval_result = evaluator.evaluate_dataframe(df)

    assert "metrics" in eval_result
    assert "summary" in eval_result
    metrics = eval_result["metrics"]
    assert isinstance(metrics, DetectionEvaluationMetrics)
    assert metrics.detector_name == "AADS"
    assert metrics.total_samples == 42
    assert metrics.true_positives >= 1
    assert metrics.recall > 0.0
    assert metrics.f1_score > 0.0
    assert 0.0 <= metrics.false_positive_rate <= 1.0
    assert eval_result["summary"]["throughput_events_per_sec"] > 0.0
