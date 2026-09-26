"""Unit tests for Stage 2 (Clustering) & Stage 3 (Minor Clusters)."""

import pytest
from src.agentic_streaming.aads.clustering import AutonomousDataPartitioner, MicroCluster
from src.agentic_streaming.aads.config import AADSConfig


def test_micro_cluster_update():
    """Verify micro-cluster centroid incremental updates."""
    cluster = MicroCluster(cluster_id=1, center=10.0, radius=2.0, step=1)
    assert cluster.support == 1
    assert cluster.center == 10.0

    cluster.update(20.0, step=2)
    assert cluster.support == 2
    assert cluster.center == 15.0


def test_partitioner_spawn_and_merge():
    """Verify partitioner merges nearby points and spawns new clusters for distant ones."""
    config = AADSConfig(cluster_radius_factor=1.0, max_minor_cluster_size=2)
    partitioner = AutonomousDataPartitioner(config)

    # Point 1: spawns cluster 1
    is_anomaly1, cid1, sup1 = partitioner.process_candidate(value=10.0, local_std=2.0)
    assert len(partitioner.clusters) == 1
    assert is_anomaly1 is True  # Support 1 <= max_minor_cluster_size

    # Point 2: within radius (dist = 1.0 <= radius 2.0) -> merges into cluster 1
    is_anomaly2, cid2, sup2 = partitioner.process_candidate(value=11.0, local_std=2.0)
    assert cid2 == cid1
    assert sup2 == 2
    assert is_anomaly2 is True  # Support 2 <= max_minor_cluster_size

    # Point 3: distant point (val = 100.0) -> spawns cluster 2
    is_anomaly3, cid3, sup3 = partitioner.process_candidate(value=100.0, local_std=2.0)
    assert cid3 != cid1
    assert len(partitioner.clusters) == 2
    assert is_anomaly3 is True


def test_stage3_minor_vs_major_cluster():
    """Verify clusters exceeding max_minor_cluster_size are categorized as non-anomalies."""
    config = AADSConfig(cluster_radius_factor=1.0, max_minor_cluster_size=3)
    partitioner = AutonomousDataPartitioner(config)

    # Add 4 points close to 50.0
    for i in range(3):
        is_anomaly, cid, sup = partitioner.process_candidate(value=50.0 + (i * 0.1), local_std=2.0)
        assert is_anomaly is True  # Support <= 3

    # 4th sample: cluster support becomes 4 (> max_minor_cluster_size) -> recognized as persistent pattern / regime shift
    is_anomaly, cid, sup = partitioner.process_candidate(value=50.2, local_std=2.0)
    assert is_anomaly is False
    assert sup == 4
