"""Stage 2 & 3 of AADS: Autonomous Data Partitioning & Minor Cluster Identification.

Grounded in:
Basheer et al., 'Autonomous Anomaly Detection for Streaming Data', KBS 2024.
Maintains evolving micro-clusters in online stream and isolates true anomalies from minor clusters.
"""

import math
from typing import Dict, List, Optional, Tuple
from src.agentic_streaming.aads.config import AADSConfig


class MicroCluster:
    """Represents an evolving micro-cluster partition in data stream space."""

    def __init__(self, cluster_id: int, center: float, radius: float, step: int) -> None:
        self.cluster_id: int = cluster_id
        self.center: float = center
        self.support: int = 1
        self.radius: float = radius
        self.created_step: int = step
        self.last_updated_step: int = step

    def update(self, value: float, step: int) -> None:
        """Incrementally update cluster centroid and support count."""
        self.center = (self.support * self.center + value) / (self.support + 1)
        self.support += 1
        self.last_updated_step = step

    def distance_to(self, value: float) -> float:
        """Euclidean distance from sample to cluster center."""
        return abs(value - self.center)


class AutonomousDataPartitioner:
    """Online clustering partitioner implementing Stage 2 and Stage 3 of AADS."""

    def __init__(self, config: AADSConfig | None = None) -> None:
        self.config = config or AADSConfig()
        self.clusters: Dict[int, MicroCluster] = {}
        self.next_cluster_id: int = 1
        self.current_step: int = 0
        self.epsilon: float = 1e-6

    def process_candidate(self, value: float, local_std: float) -> Tuple[bool, int, int]:
        """Cluster a candidate anomalous sample and determine if it belongs to a minor cluster.

        Parameters
        ----------
        value : float
            Candidate sample value.
        local_std : float
            Current estimated standard deviation of the local baseline window.

        Returns
        -------
        is_true_anomaly : bool
            True if sample falls into a minor cluster (Stage 3).
        cluster_id : int
            Identifier of assigned or spawned micro-cluster.
        cluster_support : int
            Current support (number of points) in assigned cluster.
        """
        self.current_step += 1

        # Periodically prune stale micro-clusters (streaming memory management)
        if self.current_step % 100 == 0:
            self._prune_inactive_clusters()

        # Dynamic radius based on local spread
        radius = max(local_std * self.config.cluster_radius_factor, self.epsilon)

        if not self.clusters:
            # First cluster
            cluster = self._spawn_cluster(value, radius)
            return True, cluster.cluster_id, cluster.support

        # Find nearest existing cluster
        nearest_cluster = min(self.clusters.values(), key=lambda c: c.distance_to(value))
        dist = nearest_cluster.distance_to(value)

        # Merge or Spawn
        if dist <= max(nearest_cluster.radius, radius):
            nearest_cluster.update(value, self.current_step)
            assigned_cluster = nearest_cluster
        else:
            assigned_cluster = self._spawn_cluster(value, radius)

        # Stage 3: Minor Cluster Evaluation
        is_minor = self._is_minor_cluster(assigned_cluster)

        return is_minor, assigned_cluster.cluster_id, assigned_cluster.support

    def _spawn_cluster(self, value: float, radius: float) -> MicroCluster:
        """Create and register a new micro-cluster."""
        # Enforce memory capacity bound if exceeded
        if len(self.clusters) >= self.config.max_active_clusters:
            self._evict_least_active_cluster()

        cid = self.next_cluster_id
        self.next_cluster_id += 1
        cluster = MicroCluster(cluster_id=cid, center=value, radius=radius, step=self.current_step)
        self.clusters[cid] = cluster
        return cluster

    def _is_minor_cluster(self, cluster: MicroCluster) -> bool:
        """Stage 3 check: determine whether the cluster is minor (sparse / anomalous)."""
        # Minor cluster rule 1: Support count is strictly within max_minor_cluster_size
        if cluster.support <= self.config.max_minor_cluster_size:
            return True

        # Minor cluster rule 2: Relative ratio compared to total clustered points
        total_points = sum(c.support for c in self.clusters.values())
        if total_points > 0:
            ratio = cluster.support / total_points
            if ratio < self.config.minor_cluster_ratio_threshold:
                return True

        return False

    def _prune_inactive_clusters(self) -> None:
        """Evict micro-clusters that have not received samples within idle threshold."""
        cutoff_step = self.current_step - self.config.cluster_max_idle_steps
        expired = [cid for cid, c in self.clusters.items() if c.last_updated_step < cutoff_step]
        for cid in expired:
            del self.clusters[cid]

    def _evict_least_active_cluster(self) -> None:
        """Evict oldest, least-supported cluster when memory cap is reached."""
        oldest = min(self.clusters.values(), key=lambda c: (c.support, c.last_updated_step))
        del self.clusters[oldest.cluster_id]

    def reset(self) -> None:
        """Reset clustering state."""
        self.clusters.clear()
        self.next_cluster_id = 1
        self.current_step = 0
