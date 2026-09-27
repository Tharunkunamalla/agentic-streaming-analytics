"""Controlled Tool: calculate_statistics."""

import math
from typing import List, Optional
import numpy as np

from src.agentic_streaming.tools.schemas import CalculateStatisticsInput, CalculateStatisticsOutput


def calculate_statistics_tool(inp: CalculateStatisticsInput) -> CalculateStatisticsOutput:
    """Calculate exact summary statistics and z-score for metric values."""
    vals = [float(v) for v in inp.values] if inp.values else [0.0]
    n = len(vals)
    arr = np.array(vals, dtype=float)

    mean_val = float(np.mean(arr))
    std_val = float(np.std(arr, ddof=1)) if n > 1 else 0.0
    min_val = float(np.min(arr))
    max_val = float(np.max(arr))
    median_val = float(np.median(arr))
    p95_val = float(np.percentile(arr, 95))

    target = inp.current_value if inp.current_value is not None else vals[-1]
    effective_std = max(std_val, max(0.01 * abs(mean_val), 1e-4))
    z_score = float(abs(target - mean_val) / effective_std)

    return CalculateStatisticsOutput(
        mean=round(mean_val, 4),
        std=round(std_val, 4),
        min=round(min_val, 4),
        max=round(max_val, 4),
        median=round(median_val, 4),
        p95=round(p95_val, 4),
        z_score=round(z_score, 4),
        sample_count=n,
    )
