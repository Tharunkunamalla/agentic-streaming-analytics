"""Unit tests for Phase 9 Controlled Agent Tools and Allowlisted Registry."""

import pytest
from src.agentic_streaming.tools.registry import ToolRegistry


@pytest.fixture
def registry():
    return ToolRegistry()


def test_registry_contains_all_8_tools(registry):
    tools = registry.list_tools()
    expected = [
        "calculate_statistics",
        "check_drift",
        "run_xstream",
        "run_hstree",
        "run_rrcf",
        "compare_detectors",
        "retrieve_similar_events",
        "store_decision",
    ]
    for t in expected:
        assert t in tools, f"Tool {t} missing from registry"


def test_calculate_statistics_tool(registry):
    res = registry.execute(
        "calculate_statistics",
        {"values": [10.0, 12.0, 11.0, 13.0, 15.0], "current_value": 25.0},
    )
    assert res["success"] is True
    data = res["result"]
    assert data["sample_count"] == 5
    assert data["mean"] == 12.2
    assert data["z_score"] > 2.0
    assert "latency_ms" in res


def test_check_drift_tool(registry):
    base_win = [10.0 + float(i) * 0.1 for i in range(20)]
    curr_win = [50.0 + float(i) * 0.1 for i in range(20)]
    res = registry.execute(
        "check_drift",
        {"current_window": curr_win, "baseline_window": base_win},
    )
    assert res["success"] is True
    data = res["result"]
    assert data["drift_detected"] is True
    assert data["drift_severity"] in ["HIGH_DRIFT", "MODERATE_DRIFT"]
    assert data["ks_statistic"] > 0.5


def test_single_detectors_tools(registry):
    window = [1.0, 2.0, 1.5, 1.8, 2.1, 1.9, 2.0]
    for tool_name in ["run_xstream", "run_hstree", "run_rrcf"]:
        res = registry.execute(tool_name, {"value": 15.0, "window_history": window})
        assert res["success"] is True
        data = res["result"]
        assert "anomaly_score" in data
        assert isinstance(data["is_anomaly"], bool)
        assert data["execution_time_ms"] >= 0.0


def test_compare_detectors_tool(registry):
    window = [5.0, 5.2, 4.9, 5.1, 5.0]
    res = registry.execute("compare_detectors", {"value": 25.0, "window_history": window})
    assert res["success"] is True
    data = res["result"]
    assert "scores" in data
    assert len(data["scores"]) == 4
    assert "agreement_ratio" in data
    assert isinstance(data["consensus_anomaly"], bool)


def test_unregistered_tool_fails_gracefully(registry):
    res = registry.execute("unregistered_tool", {})
    assert res["success"] is False
    assert "not allowlisted" in res["error"]


def test_invalid_tool_arguments(registry):
    res = registry.execute("calculate_statistics", {"values": "invalid_string_not_list"})
    assert res["success"] is False
    assert "Schema validation error" in res["error"]
