"""AADS Streaming Anomaly Detector: Core Orchestration of Stages 1, 2, and 3.

Authoritative Implementation based on:
Basheer et al., 'Autonomous Anomaly Detection for Streaming Data', Knowledge-Based Systems (2024).

Pipeline Flow:
Input Event
  │
  ▼
[Stage 1: Data-Density Estimation]
  ├── Density < Threshold / Z-Score >= 2.5?
  ├── NO  ──► Nominal Sample (is_anomaly = False)
  └── YES ──► Candidate Anomalous Sample
                │
                ▼
[Stage 2: Online Autonomous Data Partitioning]
  ├── Merge into nearest micro-cluster OR Spawn new micro-cluster
  │
  ▼
[Stage 3: Minor Cluster Identification]
  ├── Micro-Cluster Support <= Minor Threshold?
  ├── YES ──► True Anomaly (is_anomaly = True)
  └── NO  ──► Micro-regime Shift / Nominal Density (is_anomaly = False)
"""

import math
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional
from uuid import uuid4

from src.agentic_streaming.aads.clustering import AutonomousDataPartitioner
from src.agentic_streaming.aads.config import AADSConfig
from src.agentic_streaming.aads.density import DensityEstimator


class AADSDetector:
    """Nonparametric streaming anomaly detector implementing the 3-stage AADS algorithm."""

    def __init__(self, config: Optional[AADSConfig] = None) -> None:
        self.config = config or AADSConfig()
        # Per-metric isolated state partitions for multi-tenant streams
        self.density_estimators: Dict[str, DensityEstimator] = defaultdict(
            lambda: DensityEstimator(self.config)
        )
        self.partitioners: Dict[str, AutonomousDataPartitioner] = defaultdict(
            lambda: AutonomousDataPartitioner(self.config)
        )
        self.total_processed: int = 0
        self.total_anomalies_flagged: int = 0

    def detect_one(self, record: Any) -> Dict[str, Any]:
        """Process a single streaming event through the 3-stage AADS pipeline.

        Parameters
        ----------
        record : dict or object with attributes
            Streaming metric record containing `value`, `timestamp`, and optional `metric_id`, `event_id`.

        Returns
        -------
        result : dict
            Standardized detection report containing:
            - event_id: str
            - timestamp: float
            - anomaly_score: float
            - is_anomaly: bool
            - detector_name: str
            - detector_version: str
            - processing_time_ms: float
        """
        start_t = time.perf_counter()
        self.total_processed += 1

        # Extract record attributes safely
        if isinstance(record, dict):
            val = float(record["value"])
            ts = float(record.get("timestamp", time.time()))
            metric_id = str(record.get("metric_id", record.get("kpi_id", "default")))
            event_id = str(record.get("event_id", uuid4().hex))
        else:
            val = float(getattr(record, "value"))
            ts = float(getattr(record, "timestamp", time.time()))
            metric_id = str(getattr(record, "metric_id", getattr(record, "kpi_id", "default")))
            event_id = str(getattr(record, "event_id", uuid4().hex))

        density_est = self.density_estimators[metric_id]
        partitioner = self.partitioners[metric_id]

        # Stage 1: Data-Density-Based Identification
        is_candidate, density, anomaly_score = density_est.update(val)

        is_true_anomaly = False
        cluster_id = None
        cluster_support = 0

        # Stage 2 & Stage 3: Clustering & Minor Cluster Isolation
        if is_candidate:
            local_std = math.sqrt(density_est.variance) if density_est.variance > 0 else 1.0
            is_true_anomaly, cluster_id, cluster_support = partitioner.process_candidate(val, local_std)

        if is_true_anomaly:
            self.total_anomalies_flagged += 1

        elapsed_ms = (time.perf_counter() - start_t) * 1000.0

        return {
            "event_id": event_id,
            "timestamp": ts,
            "metric_id": metric_id,
            "value": val,
            "anomaly_score": anomaly_score,
            "density": round(density, 6),
            "is_potential_anomaly": is_candidate,
            "cluster_id": cluster_id,
            "cluster_support": cluster_support,
            "is_anomaly": is_true_anomaly,
            "detector_name": self.config.detector_name,
            "detector_version": self.config.detector_version,
            "processing_time_ms": round(elapsed_ms, 4),
        }

    def detect_batch(self, records: List[Any]) -> List[Dict[str, Any]]:
        """Process a batch of records sequentially simulating online stream arrivals."""
        return [self.detect_one(r) for r in records]

    def process_event(
        self,
        event_id: str,
        timestamp: float,
        value: float,
        metric_id: str = "default",
    ) -> Dict[str, Any]:
        """Convenience method taking explicit event fields."""
        return self.detect_one({
            "event_id": event_id,
            "timestamp": timestamp,
            "value": value,
            "metric_id": metric_id,
        })

    def fit_score(self, event: Any) -> float:
        """Standardized single-event fit and scoring interface."""
        if isinstance(event, (int, float)):
            res = self.detect_one({"value": float(event)})
        elif hasattr(event, "__len__") and len(event) == 1:
            res = self.detect_one({"value": float(event[0])})
        elif isinstance(event, dict):
            res = self.detect_one(event)
        else:
            res = self.detect_one({"value": float(getattr(event, "value", event))})
        return float(res["anomaly_score"] or (1.0 if res["is_anomaly"] else 0.0))

    def reset(self) -> None:
        """Reset internal streaming memory across all metric streams."""
        self.density_estimators.clear()
        self.partitioners.clear()
        self.total_processed = 0
        self.total_anomalies_flagged = 0
