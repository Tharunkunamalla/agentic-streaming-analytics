"""Controlled Tools for Streaming Detectors: run_xstream, run_hstree, run_rrcf, compare_detectors."""

import time
from typing import Dict, List
import numpy as np

from src.agentic_streaming.detectors.aads_adapter import AADSAdapter
from src.agentic_streaming.detectors.hstree_adapter import HSTreeAdapter
from src.agentic_streaming.detectors.rrcf_adapter import RRCFAdapter
from src.agentic_streaming.detectors.xstream_adapter import XStreamAdapter
from src.agentic_streaming.tools.schemas import (
    CompareDetectorsInput,
    CompareDetectorsOutput,
    SingleDetectorInput,
    SingleDetectorOutput,
)


def _warmup_and_evaluate(detector_adapter, value: float, history: List[float]) -> SingleDetectorOutput:
    """Warm up detector adapter with historical window and score target value."""
    t0 = time.perf_counter()
    detector_adapter.reset()

    # Pre-feed historical context
    for h in history:
        detector_adapter.fit_score(h)

    score = detector_adapter.fit_score(value)
    is_anom = detector_adapter.is_anomaly(score)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    return SingleDetectorOutput(
        detector_name=detector_adapter.name(),
        anomaly_score=round(float(score), 4),
        is_anomaly=is_anom,
        execution_time_ms=round(elapsed_ms, 3),
    )


def run_xstream_tool(inp: SingleDetectorInput) -> SingleDetectorOutput:
    """Run xStream detector tool."""
    adapter = XStreamAdapter(threshold=0.50)
    return _warmup_and_evaluate(adapter, inp.value, inp.window_history)


def run_hstree_tool(inp: SingleDetectorInput) -> SingleDetectorOutput:
    """Run HSTree detector tool."""
    adapter = HSTreeAdapter(threshold=0.50)
    return _warmup_and_evaluate(adapter, inp.value, inp.window_history)


def run_rrcf_tool(inp: SingleDetectorInput) -> SingleDetectorOutput:
    """Run RRCF detector tool."""
    adapter = RRCFAdapter(threshold=0.50)
    return _warmup_and_evaluate(adapter, inp.value, inp.window_history)


def compare_detectors_tool(inp: CompareDetectorsInput) -> CompareDetectorsOutput:
    """Run all 4 detectors in parallel/sequence and compute consensus agreement."""
    t0 = time.perf_counter()

    res_aads = _warmup_and_evaluate(AADSAdapter(threshold=0.50), inp.value, inp.window_history)
    res_hstree = run_hstree_tool(SingleDetectorInput(value=inp.value, window_history=inp.window_history))
    res_xstream = run_xstream_tool(SingleDetectorInput(value=inp.value, window_history=inp.window_history))
    res_rrcf = run_rrcf_tool(SingleDetectorInput(value=inp.value, window_history=inp.window_history))

    scores = {
        "AADS": res_aads.anomaly_score,
        "HSTree": res_hstree.anomaly_score,
        "xStream": res_xstream.anomaly_score,
        "RRCF": res_rrcf.anomaly_score,
    }

    anom_flags = [res_aads.is_anomaly, res_hstree.is_anomaly, res_xstream.is_anomaly, res_rrcf.is_anomaly]
    num_anom = sum(1 for f in anom_flags if f)
    agreement_ratio = float(num_anom / len(anom_flags))
    consensus_anomaly = num_anom >= 2

    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    return CompareDetectorsOutput(
        scores=scores,
        consensus_anomaly=consensus_anomaly,
        agreement_ratio=round(agreement_ratio, 4),
        execution_time_ms=round(elapsed_ms, 3),
    )
