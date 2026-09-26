"""Stage 1 of AADS: Data-Density-Based Identification of Potentially Anomalous Samples.

Grounded in:
Basheer et al., 'Autonomous Anomaly Detection for Streaming Data', KBS 2024.
Uses empirical data analytics (EDA) spatial density estimation over a dynamic streaming baseline.
"""

import math
from collections import deque
from typing import Tuple
from src.agentic_streaming.aads.config import AADSConfig


class DensityEstimator:
    """Nonparametric streaming data density estimator for Stage 1 candidate filtering."""

    def __init__(self, config: AADSConfig | None = None) -> None:
        self.config = config or AADSConfig()
        self.window: deque[float] = deque(maxlen=self.config.density_window_size)
        self.mean: float = 0.0
        self.variance: float = 0.0
        self.sample_count: int = 0
        self.epsilon: float = 1e-6

    def update(self, value: float) -> Tuple[bool, float, float]:
        """Process a single streaming observation.

        Parameters
        ----------
        value : float
            Observed time-series value.

        Returns
        -------
        is_potential_anomaly : bool
            True if sample density deviates significantly from baseline.
        density : float
            Estimated Cauchy/EDA data density in (0.0, 1.0].
        anomaly_score : float
            Continuous normalized distance score (z-score / deviation).
        """
        self.sample_count += 1
        self.window.append(value)

        # Warm-up phase
        if len(self.window) < self.config.min_warmup_samples:
            self._update_statistics_window()
            return False, 1.0, 0.0

        # Compute empirical baseline statistics
        self._update_statistics_window()
        # Scale-aware standard deviation floor (at least 1% of mean magnitude or epsilon)
        scale_floor = max(0.01 * abs(self.mean), 1e-3)
        effective_std = max(math.sqrt(self.variance), scale_floor)
        effective_var = max(self.variance, scale_floor ** 2)

        # Spatial distance and normalized deviation
        diff = abs(value - self.mean)
        anomaly_score = diff / effective_std

        # Cauchy-Lorentz empirical data density: D(x) = 1 / (1 + (x - mu)^2 / (sigma^2 + eps))
        density = 1.0 / (1.0 + (diff ** 2) / effective_var)

        # Potential anomaly condition
        is_potential_anomaly = anomaly_score >= self.config.density_z_threshold

        return is_potential_anomaly, density, round(anomaly_score, 4)

    def _update_statistics_window(self) -> None:
        """Compute rolling sample mean and variance from current sliding window."""
        n = len(self.window)
        if n == 0:
            return
        self.mean = sum(self.window) / n
        if n > 1:
            self.variance = sum((x - self.mean) ** 2 for x in self.window) / (n - 1)
        else:
            self.variance = 0.0

    def reset(self) -> None:
        """Reset internal streaming state."""
        self.window.clear()
        self.mean = 0.0
        self.variance = 0.0
        self.sample_count = 0
