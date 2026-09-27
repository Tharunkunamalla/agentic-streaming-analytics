"""SQLite Database Storage Manager for Telemetry, Decisions, and Tool Executions.

Creates and manages tables for:
- events
- detections
- agent_decisions
- tool_executions
- experiments
- metrics
"""

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Optional
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent


class RelationalStorageManager:
    """Thread-safe SQLite storage for experiment metrics, tool executions, and decisions."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        if db_path is None:
            db_dir = REPO_ROOT / "data"
            db_dir.mkdir(parents=True, exist_ok=True)
            self.db_path = str(db_dir / "streaming_analytics.db")
        else:
            self.db_path = str(db_path)

        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Initialize relational database schema for Phase 11 & Phase 12."""
        with self._get_connection() as conn:
            # 1. events
            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    metric_id TEXT NOT NULL,
                    value REAL NOT NULL,
                    features_json TEXT NOT NULL
                )
            """)

            # 2. detections
            conn.execute("""
                CREATE TABLE IF NOT EXISTS detections (
                    detection_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    detector TEXT NOT NULL,
                    score REAL NOT NULL,
                    prediction INTEGER NOT NULL,
                    latency_ms REAL NOT NULL,
                    timestamp TEXT NOT NULL,
                    FOREIGN KEY(event_id) REFERENCES events(event_id)
                )
            """)

            # 3. agent_decisions
            conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_decisions (
                    decision_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    latency_ms REAL NOT NULL,
                    active_detector TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    FOREIGN KEY(event_id) REFERENCES events(event_id)
                )
            """)

            # 4. tool_executions
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tool_executions (
                    execution_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    tool TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    result_json TEXT NOT NULL,
                    latency_ms REAL NOT NULL,
                    FOREIGN KEY(event_id) REFERENCES events(event_id)
                )
            """)

            # 5. experiments
            conn.execute("""
                CREATE TABLE IF NOT EXISTS experiments (
                    experiment_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    config_json TEXT NOT NULL,
                    status TEXT NOT NULL
                )
            """)

            # 6. metrics
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    metric_id TEXT PRIMARY KEY,
                    experiment_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    precision REAL NOT NULL,
                    recall REAL NOT NULL,
                    f1 REAL NOT NULL,
                    fpr REAL NOT NULL,
                    latency_ms REAL NOT NULL,
                    throughput_eps REAL NOT NULL,
                    active_detector TEXT NOT NULL,
                    FOREIGN KEY(experiment_id) REFERENCES experiments(experiment_id)
                )
            """)

            conn.commit()

    def log_event(self, event_id: str, timestamp: str, metric_id: str, value: float, features_json: str) -> None:
        with self._get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO events (event_id, timestamp, metric_id, value, features_json) VALUES (?, ?, ?, ?, ?)",
                (event_id, str(timestamp), metric_id, float(value), features_json),
            )
            conn.commit()

    def log_detection(
        self, event_id: str, detector: str, score: float, prediction: bool, latency_ms: float, timestamp: str
    ) -> str:
        det_id = f"det-{uuid4().hex[:8]}"
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO detections (detection_id, event_id, detector, score, prediction, latency_ms, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (det_id, event_id, detector, float(score), 1 if prediction else 0, float(latency_ms), str(timestamp)),
            )
            conn.commit()
        return det_id

    def log_agent_decision(
        self,
        event_id: str,
        action: str,
        reason: str,
        success: bool,
        latency_ms: float,
        active_detector: str,
        timestamp: str,
    ) -> str:
        dec_id = f"dec-{uuid4().hex[:8]}"
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO agent_decisions (decision_id, event_id, action, reason, success, latency_ms, active_detector, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    dec_id,
                    event_id,
                    action,
                    reason,
                    1 if success else 0,
                    float(latency_ms),
                    active_detector,
                    str(timestamp),
                ),
            )
            conn.commit()
        return dec_id

    def log_tool_execution(
        self,
        event_id: str,
        tool: str,
        start_time: str,
        end_time: str,
        success: bool,
        result_json: str,
        latency_ms: float,
    ) -> str:
        exec_id = f"exec-{uuid4().hex[:8]}"
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO tool_executions (execution_id, event_id, tool, start_time, end_time, success, result_json, latency_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    exec_id,
                    event_id,
                    tool,
                    str(start_time),
                    str(end_time),
                    1 if success else 0,
                    result_json,
                    float(latency_ms),
                ),
            )
            conn.commit()
        return exec_id

    def clear_all(self) -> None:
        """Clear database tables (used for test setup)."""
        with self._get_connection() as conn:
            for tbl in ["events", "detections", "agent_decisions", "tool_executions", "experiments", "metrics"]:
                conn.execute(f"DELETE FROM {tbl}")
            conn.commit()
