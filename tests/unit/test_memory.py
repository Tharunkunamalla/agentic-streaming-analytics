"""Unit tests for Phase 10 Durable SQLite Memory and Memory Tools."""

from pathlib import Path
import pytest
from src.agentic_streaming.memory.sqlite_memory import SQLiteMemoryManager
from src.agentic_streaming.tools.registry import ToolRegistry


@pytest.fixture
def memory_db(tmp_path):
    db_file = tmp_path / "test_memory.db"
    mem = SQLiteMemoryManager(db_path=db_file)
    yield mem
    mem.clear()


def test_sqlite_memory_store_and_retrieve(memory_db):
    mem_id = memory_db.store_decision(
        event_id="evt-001",
        timestamp="2026-09-27T23:00:00Z",
        action="CHECK_DRIFT",
        tool="check_drift",
        result='{"drift_detected": true}',
        success=True,
        latency_ms=12.5,
        event_type="HIGH_ANOMALY",
        detector="AADS",
        anomaly_score=0.91,
        metric_id="kpi-01",
    )
    assert mem_id.startswith("mem-")

    retrieved = memory_db.retrieve_similar_events(metric_id="kpi-01", anomaly_score=0.90, limit=5)
    assert len(retrieved) == 1
    record = retrieved[0]
    assert record["event_id"] == "evt-001"
    assert record["action"] == "CHECK_DRIFT"
    assert record["tool"] == "check_drift"
    assert record["success"] is True
    assert record["latency_ms"] == 12.5
    assert record["detector"] == "AADS"


def test_memory_tools_via_registry(tmp_path, monkeypatch):
    test_db = tmp_path / "registry_memory.db"
    mem_mgr = SQLiteMemoryManager(db_path=test_db)
    
    # Patch singleton getter in memory_tools
    monkeypatch.setattr(
        "src.agentic_streaming.tools.memory_tools.get_memory_manager",
        lambda: mem_mgr,
    )

    registry = ToolRegistry()

    # 1. Store Decision via Registry
    store_res = registry.execute(
        "store_decision",
        {
            "event_id": "evt-100",
            "timestamp": "2026-09-27T23:15:00Z",
            "action": "COMPARE_DETECTORS",
            "tool": "compare_detectors",
            "result": '{"consensus_anomaly": true}',
            "success": True,
            "latency_ms": 25.4,
            "detector": "AADS",
            "event_type": "ANOMALY_CANDIDATE",
        },
    )
    assert store_res["success"] is True
    assert store_res["result"]["saved"] is True

    # 2. Retrieve Similar Events via Registry
    ret_res = registry.execute(
        "retrieve_similar_events",
        {"metric_id": "default", "anomaly_score": 0.85, "limit": 3},
    )
    assert ret_res["success"] is True
    assert ret_res["result"]["count"] == 1
    events = ret_res["result"]["similar_events"]
    assert events[0]["event_id"] == "evt-100"
    assert events[0]["action"] == "COMPARE_DETECTORS"
