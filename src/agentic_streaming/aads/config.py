"""Configuration settings and parameters for the AADS streaming baseline detector.

Grounded in:
'Autonomous Anomaly Detection for Streaming Data', Knowledge-Based Systems (2024).
All engineering assumptions are configurable with explicit bounds.
"""

from pydantic import BaseModel, ConfigDict, Field


class AADSConfig(BaseModel):
    """Hyperparameters and operational limits for the 3-stage AADS detector."""

    model_config = ConfigDict(frozen=True)

    # --- Stage 1: Data-Density Identification ---
    density_window_size: int = Field(
        default=100, ge=10, le=10000,
        description="Sliding window size for density baseline statistics",
    )
    density_z_threshold: float = Field(
        default=2.5, gt=0.0, le=10.0,
        description="Z-score / distance threshold to flag candidate anomalous samples",
    )
    decay_alpha: float = Field(
        default=0.05, gt=0.0, le=1.0,
        description="Exponential moving average decay factor for non-stationary statistics",
    )
    min_warmup_samples: int = Field(
        default=30, ge=5,
        description="Minimum observations before density filtering activates",
    )

    # --- Stage 2: Autonomous Data Partitioning (Online Clustering) ---
    cluster_radius_factor: float = Field(
        default=0.5, gt=0.0, le=5.0,
        description="Scaling factor gamma applied to window stddev for micro-cluster radius",
    )
    max_active_clusters: int = Field(
        default=100, ge=10, le=1000,
        description="Maximum active micro-clusters retained in memory per metric stream",
    )
    cluster_max_idle_steps: int = Field(
        default=500, ge=50,
        description="Time steps after which an inactive micro-cluster is decayed/pruned",
    )

    # --- Stage 3: Minor Cluster Identification ---
    max_minor_cluster_size: int = Field(
        default=3, ge=1, le=50,
        description="Maximum member count for a micro-cluster to be categorized as minor (true anomaly)",
    )
    minor_cluster_ratio_threshold: float = Field(
        default=0.05, gt=0.0, le=0.5,
        description="Maximum support ratio relative to active window to qualify as minor cluster",
    )

    # --- Detector Metadata ---
    detector_name: str = Field(
        default="AADS",
        description="Algorithm identification",
    )
    detector_version: str = Field(
        default="1.0.0-kbs2024",
        description="Paper implementation version",
    )
