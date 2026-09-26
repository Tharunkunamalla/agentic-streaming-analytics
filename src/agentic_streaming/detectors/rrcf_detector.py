"""Robust Random Cut Forest (RRCF) Streaming Anomaly Detector.

Reference:
    Guha, S., Mishra, N., Roy, G., & Schrijvers, O. (2016).
    Robust random cut forest based anomaly detection on streams.
    In International Conference on Machine Learning (ICML) (pp. 2712-2721).
"""

from collections import deque
import math
from typing import Any, Deque, Dict, List, Optional, Tuple
import numpy as np

from src.agentic_streaming.detectors.base import BaseStreamingDetector


class RRCFNode:
    """Node in a 1D Random Cut Tree."""

    def __init__(
        self,
        min_val: float,
        max_val: float,
        point_index: Optional[int] = None,
        left: Optional["RRCFNode"] = None,
        right: Optional["RRCFNode"] = None,
    ) -> None:
        self.min_val = min_val
        self.max_val = max_val
        self.point_index = point_index  # Set only on leaf nodes
        self.left = left
        self.right = right
        self.num_leaves: int = 1 if point_index is not None else 0
        if left and right:
            self.num_leaves = left.num_leaves + right.num_leaves


class RRCTree:
    """A single 1-D Random Cut Tree maintaining a sliding window of points."""

    def __init__(self, rng: np.random.RandomState) -> None:
        self.rng = rng
        self.root: Optional[RRCFNode] = None

    def insert_point(self, val: float, idx: int) -> None:
        """Insert a 1D scalar observation into the tree."""
        leaf = RRCFNode(min_val=val, max_val=val, point_index=idx)
        if self.root is None:
            self.root = leaf
            return

        curr = self.root
        parent: Optional[RRCFNode] = None
        is_left_child = False

        # Traverse down to insert
        while curr.left is not None and curr.right is not None:
            curr.min_val = min(curr.min_val, val)
            curr.max_val = max(curr.max_val, val)
            curr.num_leaves += 1

            # Decide split direction
            left_dist = abs(val - (curr.left.min_val + curr.left.max_val) / 2.0)
            right_dist = abs(val - (curr.right.min_val + curr.right.max_val) / 2.0)

            parent = curr
            if left_dist <= right_dist:
                curr = curr.left
                is_left_child = True
            else:
                curr = curr.right
                is_left_child = False

        # Create new branch
        new_min = min(curr.min_val, val)
        new_max = max(curr.max_val, val)
        branch = RRCFNode(min_val=new_min, max_val=new_max, left=curr, right=leaf)
        branch.num_leaves = curr.num_leaves + 1

        if parent is None:
            self.root = branch
        else:
            if is_left_child:
                parent.left = branch
            else:
                parent.right = branch

    def compute_codisp(self, val: float) -> float:
        """Calculate Collusive Displacement (CoDisp) score for candidate value."""
        if self.root is None or self.root.num_leaves < 3:
            return 0.0

        # If point lies outside current tree bounding box, its displacement is high
        if val < self.root.min_val or val > self.root.max_val:
            span = max(self.root.max_val - self.root.min_val, 1e-4)
            out_dist = max(self.root.min_val - val, val - self.root.max_val)
            return float(min(1.0, 0.75 + 0.25 * (out_dist / (span + out_dist))))

        curr = self.root
        max_codisp = 0.0

        # Traverse tree towards val
        while curr is not None and curr.left is not None and curr.right is not None:
            left_center = (curr.left.min_val + curr.left.max_val) / 2.0
            right_center = (curr.right.min_val + curr.right.max_val) / 2.0

            if abs(val - left_center) <= abs(val - right_center):
                # Candidate goes left; sibling is right
                sibling_leaves = curr.right.num_leaves
                curr = curr.left
            else:
                sibling_leaves = curr.left.num_leaves
                curr = curr.right

            codisp = sibling_leaves / max(curr.num_leaves, 1)
            if codisp > max_codisp:
                max_codisp = codisp

        return float(max_codisp)


class RRCFDetector(BaseStreamingDetector):
    """Streaming ensemble of Robust Random Cut Trees."""

    def __init__(
        self,
        num_trees: int = 25,
        tree_size: int = 150,
        threshold: float = 0.85,
        random_seed: int = 42,
    ) -> None:
        super().__init__(name="RRCF", version="1.0.0")
        self.num_trees = num_trees
        self.tree_size = tree_size
        self.threshold = threshold
        self.random_seed = random_seed

        self.rng = np.random.RandomState(random_seed)
        self.trees: List[RRCTree] = [RRCTree(np.random.RandomState(self.rng.randint(0, 100000))) for _ in range(num_trees)]
        self.window_buffer: Deque[float] = deque(maxlen=tree_size)
        self.seq_id: int = 0

    def score(self, value: float, timestamp: Optional[float] = None) -> float:
        """Compute average CoDisp across random cut trees."""
        if len(self.window_buffer) < 5:
            return 0.0

        codisps = [tree.compute_codisp(value) for tree in self.trees]
        avg_codisp = float(np.mean(codisps)) if codisps else 0.0
        return float(np.clip(avg_codisp, 0.0, 1.0))

    def update(self, value: float, timestamp: Optional[float] = None) -> None:
        """Insert new point into trees and re-balance when window fills."""
        self.seq_id += 1
        self.window_buffer.append(value)

        # Every tree_size steps, re-seed trees from current window to maintain tight bounding boxes
        if len(self.window_buffer) >= self.tree_size and self.seq_id % (self.tree_size // 2) == 0:
            self._rebuild_trees()
        else:
            for tree in self.trees:
                tree.insert_point(value, self.seq_id)

    def _rebuild_trees(self) -> None:
        """Rebuild trees from sliding window buffer."""
        window_pts = list(self.window_buffer)
        self.trees = [RRCTree(np.random.RandomState(self.rng.randint(0, 100000))) for _ in range(self.num_trees)]
        for i, pt in enumerate(window_pts):
            for tree in self.trees:
                tree.insert_point(pt, i)

    def is_anomaly(self, score: float) -> bool:
        """Check if normalized CoDisp exceeds threshold."""
        return score >= self.threshold

    def reset(self) -> None:
        """Reset forest."""
        self.seq_id = 0
        self.total_processed = 0
        self.window_buffer.clear()
        self.trees = [RRCTree(np.random.RandomState(self.rng.randint(0, 100000))) for _ in range(self.num_trees)]

    def get_metadata(self) -> Dict[str, Any]:
        meta = super().get_metadata()
        meta.update({
            "num_trees": self.num_trees,
            "tree_size": self.tree_size,
            "threshold": self.threshold,
        })
        return meta
