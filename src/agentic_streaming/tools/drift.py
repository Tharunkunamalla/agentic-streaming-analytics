"""Controlled Tool: check_drift."""

import numpy as np
from scipy import stats

from src.agentic_streaming.tools.schemas import CheckDriftInput, CheckDriftOutput


def calculate_psi(baseline: np.ndarray, current: np.ndarray, num_bins: int = 10) -> float:
    """Calculate Population Stability Index (PSI) between baseline and current distributions."""
    if len(baseline) == 0 or len(current) == 0:
        return 0.0

    min_v = min(np.min(baseline), np.min(current))
    max_v = max(np.max(baseline), np.max(current))
    if min_v == max_v:
        return 0.0

    bins = np.linspace(min_v, max_v, num_bins + 1)
    base_counts, _ = np.histogram(baseline, bins=bins)
    curr_counts, _ = np.histogram(current, bins=bins)

    base_pct = (base_counts + 1e-4) / (len(baseline) + 1e-4 * num_bins)
    curr_pct = (curr_counts + 1e-4) / (len(current) + 1e-4 * num_bins)

    psi_val = np.sum((curr_pct - base_pct) * np.log(curr_pct / base_pct))
    return float(psi_val)


def check_drift_tool(inp: CheckDriftInput) -> CheckDriftOutput:
    """Execute KS-test and PSI statistical distribution drift check."""
    curr_arr = np.array(inp.current_window if inp.current_window else [0.0], dtype=float)
    base_arr = np.array(inp.baseline_window if inp.baseline_window else [0.0], dtype=float)

    if len(curr_arr) < 2 or len(base_arr) < 2:
        return CheckDriftOutput(
            drift_detected=False,
            ks_statistic=0.0,
            p_value=1.0,
            psi=0.0,
            drift_severity="NO_DRIFT",
        )

    # Kolmogorov-Smirnov 2-sample test
    ks_res = stats.ks_2samp(curr_arr, base_arr)
    ks_stat = float(ks_res.statistic)
    p_val = float(ks_res.pvalue)

    # Population Stability Index
    psi_val = calculate_psi(base_arr, curr_arr)

    # Drift decision logic
    drift_detected = p_val < 0.05 or psi_val > 0.25

    if psi_val > 0.25 or (p_val < 0.01 and ks_stat > 0.4):
        severity = "HIGH_DRIFT"
    elif psi_val > 0.10 or p_val < 0.05:
        severity = "MODERATE_DRIFT"
    else:
        severity = "NO_DRIFT"

    return CheckDriftOutput(
        drift_detected=drift_detected,
        ks_statistic=round(ks_stat, 4),
        p_value=round(p_val, 4),
        psi=round(psi_val, 4),
        drift_severity=severity,
    )
