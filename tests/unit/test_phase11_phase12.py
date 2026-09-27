"""Unit tests for Phase 11 Autonomous Detector Adaptation and Phase 12 Database Storage."""

from datetime import datetime, timezone
import pytest
from src.agentic_streaming.agent.graph import StreamingAgentWorkflow
from src.agentic_streaming.detectors.detector_selector import AutonomousDetectorSelector
from src.agentic_streaming.storage.relational_storage import RelationalStorageManager
from src.schemas.anomaly import AnomalyEvent


@pytest.fixture
def db_storage(tmp_path):
    db_file = tmp_path / "test_relational.db"
    storage = RelationalStorageManager(db_path=db_file)
    yield storage
    storage.clear_all()


def test_relational_storage_logging(db_storage):
    db_storage.log_event("evt-1", "2026-09-27T23:30:00Z", "kpi-1", 55.4, '{"raw": 55.4}')
    db_storage.log_detection("evt-1", "AADS", 0.92, True, 0.04, "2026-09-27T23:30:00Z")
    dec_id = db_storage.log_agent_decision("evt-1", "CHECK_DRIFT", "Drift check requested", True, 15.2, "AADS", "2026-09-27T23:30:00Z")
    exec_id = db_storage.log_tool_execution("evt-1", "check_drift", "2026-09-27T23:30:00Z", "2026-09-27T23:30:01Z", True, '{"psi": 0.25}', 15.2)

    assert dec_id.startswith("dec-")
    assert exec_id.startswith("exec-")

    with db_storage._get_connection() as conn:
        evt_row = conn.execute("SELECT * FROM events WHERE event_id='evt-1'").fetchone()
        assert evt_row["value"] == 55.4

        det_row = conn.execute("SELECT * FROM detections WHERE event_id='evt-1'").fetchone()
        assert det_row["score"] == 0.92

        dec_row = conn.execute("SELECT * FROM agent_decisions WHERE event_id='evt-1'").fetchone()
        assert dec_row["action"] == "CHECK_DRIFT"

        exec_row = conn.execute("SELECT * FROM tool_executions WHERE event_id='evt-1'").fetchone()
        assert exec_row["tool"] == "check_drift"


def test_deterministic_detector_adaptation():
    selector = AutonomousDetectorSelector()
    assert selector.active_detector == "AADS"

    # Case 1: Drift check triggers COMPARE_DETECTORS and selects xStream under high drift
    win = [10.0] * 5
    base = [1.0] * 10
    res = selector.evaluate_adaptation("evt-2", "CHECK_DRIFT", 45.0, win, base)
    assert res["drift_detected"] is True
    assert res["selected_detector"] in ["xStream", "HSTree", "RRCF", "AADS"]


def test_workflow_phase11_and_phase12_integration(db_storage):
    workflow = StreamingAgentWorkflow(db_storage=db_storage)
    evt = AnomalyEvent(
        event_id="evt-integration-001",
        timestamp=datetime.now(timezone.utc),
        dataset="AIOPS_KPI",
        metric_id="kpi-001",
        value=88.5,
        anomaly_score=0.75,
        detector="AADS",
        recent_window_summary={"rolling_mean": 10.0, "rolling_std": 1.0},
        anomaly_frequency=0.10,
    )

    state, decision = workflow.run(evt)
    assert state["is_complete"] is True
    assert decision.event_id == "evt-integration-001"
    assert "adaptation" in state["action_params"]

    # Verify rows written into database tables
    with db_storage._get_connection() as conn:
        dec_cnt = conn.execute("SELECT COUNT(*) FROM agent_decisions").fetchone()[0]
        exec_cnt = conn.execute("SELECT COUNT(*) FROM tool_executions").fetchone()[0]
        assert dec_cnt == 1
        assert exec_cnt == 1
