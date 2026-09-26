"""Half-Space Trees (HSTree) Streaming Anomaly Detector.

Reference:
    Tan, S. C., Ting, K. M., & Zhou, Z. H. (2011).
    Fast anomaly detection for streaming data.
    In Proceedings of the 17th ACM SIGKDD International Conference on Knowledge
    Discovery and Data Mining (pp. 1411-1419).
"""

import math
from typing import Any, Dict, List, Optional
import numpy as np

from src.agentic_streaming.detectors.base import BaseStreamingDetector


class HSNode:
    """Node in a Half-Space Tree partitioning a 1-D space."""

    def __init__(self, depth: int, min_val: float, max_val: float) -> None:
        self.depth = depth
        self.min_val = min_val
        self.max_val = max_val
        self.split_val = (min_val + max_val) / 2.0
        self.r_mass: float = 0.0  # Reference mass from prior window
        self.l_mass: float = 0.0  # Latest mass in current window
        self.left: Optional["HSNode"] = None
        self.right: Optional["HSNode"] = None

    def build_tree(self, max_depth: int) -> None:
        """Recursively build balanced binary tree down to max_depth."""
        if self.depth >= max_depth:
            return
        self.left = HSNode(self.depth + 1, self.min_val, self.split_val)
        self.right = HSNode(self.depth + 1, self.split_val, self.max_val)
        self.left.build_tree(max_depth)
        self.right.build_tree(max_depth)


class HSTreeDetector(BaseStreamingDetector):
    """Fast streaming ensemble of Half-Space Trees."""

    def __init__(
        self,
        n_trees: int = 25,
        max_depth: int = 8,
        window_size: int = 200,
        threshold: float = 0.85,
        initial_min: float = -100.0,
        initial_max: float = 100.0,
        random_seed: int = 42,
    ) -> None:
        super().__init__(name="HSTree", version="1.0.0")
        self.n_trees = n_trees
        self.max_depth = max_depth
        self.window_size = window_size
        self.threshold = threshold
        self.initial_min = initial_min
        self.initial_max = initial_max
        self.random_seed = random_seed

        self.rng = np.random.RandomState(random_seed)
        self.trees: List[HSNode] = []
        self.window_count: int = 0
        self._init_trees()

    def _init_trees(self) -> None:
        """Initialize random tree ensemble."""
        self.trees = []
        span = self.initial_max - self.initial_min
        if span <= 0:
            span = 200.0
            self.initial_min = -100.0
            self.initial_max = 100.0

        for _ in range(self.n_trees):
            # Introduce slight randomized perturbations to workspace limits for ensemble diversity
            perturb = self.rng.uniform(-0.2, 0.2) * span
            s_min = self.initial_min + perturb
            s_max = self.initial_max + perturb
            root = HSNode(depth=0, min_val=s_min, max_val=s_max)
            root.build_tree(self.max_depth)
            self.trees.append(root)

    def score(self, value: float, timestamp: Optional[float] = None) -> float:
        """Compute anomaly score by evaluating reference mass density along tree paths."""
        if not self.trees:
            return 0.0

        total_score = 0.0
        # In HSTree, anomaly score = sum over trees of sum over path of r_mass * 2^depth
        max_possible = self.n_trees * (self.window_size + 1) * (2 ** (self.max_depth + 1))

        for root in self.trees:
            node = root
            while node is not None:
                # Dense regions accumulate high mass; sparse / outlier regions accumulate low mass
                total_score += node.r_mass * (2 ** node.depth)
                if node.left is None or node.right is None:
                    break
                if value <= node.split_val:
                    node = node.left
                else:
                    node = node.right

        # Invert mass into anomaly score: low mass -> high anomaly score
        normalized_density = min(total_score / max(max_possible, 1.0), 1.0)
        anomaly_score = 1.0 - normalized_density
        return float(np.clip(anomaly_score, 0.0, 1.0))

    def update(self, value: float, timestamp: Optional[float] = None) -> None:
        """Increment latest mass along traversal path and perform window rollovers."""
        for root in self.trees:
            node = root
            while node is not None:
                node.l_mass += 1.0
                if node.left is None or node.right is None:
                    break
                if value <= node.split_val:
                    node = node.left
                else:
                    node = node.right

        self.window_count += 1
        if self.window_count >= self.window_size:
            self._shift_window()
            self.window_count = 0

    def _shift_window(self) -> None:
        """Rotate latest mass into reference mass across all trees."""
        for root in self.trees:
            stack = [root]
            while stack:
                node = stack.pop()
                node.r_mass = node.l_mass
                node.l_mass = 0.0
                if node.left:
                    stack.append(node.left)
                if node.right:
                    stack.append(node.right)

    def is_anomaly(self, score: float) -> bool:
        """Flag anomaly if score exceeds configurable threshold."""
        return score >= self.threshold

    def reset(self) -> None:
        """Reset ensemble and window counts."""
        self.window_count = 0
        self.total_processed = 0
        self._init_trees()

    def get_metadata(self) -> Dict[str, Any]:
        meta = super().get_metadata()
        meta.update({
            "n_trees": self.n_trees,
            "max_depth": self.max_depth,
            "window_size": self.window_size,
            "threshold": self.threshold,
        })
        return meta
