"""xStream Streaming Anomaly Detector.

Reference:
    Manzoor, E., Leman, A., & Akoglu, L. (2018).
    xStream: Outlier Detection in Feature-Evolving Data Streams.
    In Proceedings of the 24th ACM SIGKDD International Conference on Knowledge
    Discovery & Data Mining (pp. 1963-1972).
"""

from collections import defaultdict
import math
from typing import Any, Dict, List, Optional
import numpy as np

from src.agentic_streaming.detectors.base import BaseStreamingDetector


class XStreamDetector(BaseStreamingDetector):
    """Streaming multi-projection density estimator for outlier detection."""

    def __init__(
        self,
        n_chains: int = 20,
        depth: int = 5,
        window_size: int = 250,
        threshold: float = 0.85,
        random_seed: int = 42,
    ) -> None:
        super().__init__(name="xStream", version="1.0.0")
        self.n_chains = n_chains
        self.depth = depth
        self.window_size = window_size
        self.threshold = threshold
        self.random_seed = random_seed

        self.rng = np.random.RandomState(random_seed)
        # Random projections and shift offsets for each chain and depth level
        self.projections = self.rng.normal(loc=0.0, scale=1.0, size=(n_chains, depth))
        self.shifts = self.rng.uniform(low=0.0, high=1.0, size=(n_chains, depth))
        self.bin_widths = np.array([2.0 ** (-d) for d in range(depth)])

        # Streaming hash tables: chain_id -> depth -> bin_id -> float count
        self.counts: List[List[Dict[int, float]]] = [
            [defaultdict(float) for _ in range(depth)] for _ in range(n_chains)
        ]
        self.decay_factor: float = math.pow(0.5, 1.0 / max(window_size, 10))

    def _get_bins(self, value: float) -> List[List[int]]:
        """Compute quantized bin coordinates across all chains and depths."""
        all_bins = []
        for c in range(self.n_chains):
            chain_bins = []
            for d in range(self.depth):
                proj_val = value * self.projections[c, d]
                bin_idx = int(math.floor((proj_val - self.shifts[c, d]) / self.bin_widths[d]))
                chain_bins.append(bin_idx)
            all_bins.append(chain_bins)
        return all_bins

    def score(self, value: float, timestamp: Optional[float] = None) -> float:
        """Compute anomaly score based on average bin frequency across random chains."""
        if self.total_processed < 5:
            return 0.0

        all_bins = self._get_bins(value)
        chain_scores = []

        for c in range(self.n_chains):
            # Evaluate depth frequency
            depth_counts = []
            for d in range(self.depth):
                bin_idx = all_bins[c][d]
                c_val = self.counts[c][d].get(bin_idx, 0.0)
                depth_counts.append(c_val)

            # Average frequency in this chain
            avg_count = float(np.mean(depth_counts)) if depth_counts else 0.0
            # Low count implies isolated, anomalous region
            score_chain = 1.0 / (1.0 + math.log1p(max(avg_count, 0.0)))
            chain_scores.append(score_chain)

        mean_anomaly_score = float(np.mean(chain_scores)) if chain_scores else 0.0
        return float(np.clip(mean_anomaly_score, 0.0, 1.0))

    def update(self, value: float, timestamp: Optional[float] = None) -> None:
        """Increment bin counts with exponential window decay."""
        all_bins = self._get_bins(value)

        # Decay prior counts if necessary every 10 steps to save overhead
        if self.total_processed % 10 == 0:
            factor = self.decay_factor ** 10
            for c in range(self.n_chains):
                for d in range(self.depth):
                    for k in list(self.counts[c][d].keys()):
                        self.counts[c][d][k] *= factor
                        if self.counts[c][d][k] < 1e-4:
                            del self.counts[c][d][k]

        # Add new observation
        for c in range(self.n_chains):
            for d in range(self.depth):
                bin_idx = all_bins[c][d]
                self.counts[c][d][bin_idx] += 1.0

    def is_anomaly(self, score: float) -> bool:
        """Evaluate if score exceeds anomaly threshold."""
        return score >= self.threshold

    def reset(self) -> None:
        """Reset internal chain counts."""
        self.total_processed = 0
        self.counts = [
            [defaultdict(float) for _ in range(self.depth)] for _ in range(self.n_chains)
        ]

    def get_metadata(self) -> Dict[str, Any]:
        meta = super().get_metadata()
        meta.update({
            "n_chains": self.n_chains,
            "depth": self.depth,
            "window_size": self.window_size,
            "threshold": self.threshold,
        })
        return meta
