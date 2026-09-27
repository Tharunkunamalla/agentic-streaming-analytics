"""Durable SQLite Memory Manager for Agent Decisions and Episodic Retrieval."""

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Optional
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent


class SQLiteMemoryManager:
    """Lightweight, thread-safe SQLite memory store for agent decision history."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        if db_path is None:
            db_dir = REPO_ROOT / "data"
            db_dir.mkdir(parents=True, exist_ok=True)
            self.db_path = str(db_dir / "memory.db")
        else:
            self.db_path = str(db_path)

        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Create agent_decisions table if not exists."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_decisions (
                    memory_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    action TEXT NOT NULL,
                    tool TEXT NOT NULL,
                    result TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    latency_ms REAL NOT NULL,
                    event_type TEXT NOT NULL,
                    detector TEXT NOT NULL,
                    anomaly_score REAL DEFAULT 0.0,
                    metric_id TEXT DEFAULT 'default'
                )
            """)
            conn.commit()

    def store_decision(
        self,
        event_id: str,
        timestamp: str,
        action: str,
        tool: str,
        result: str,
        success: bool = True,
        latency_ms: float = 0.0,
        event_type: str = "ANOMALY_CANDIDATE",
        detector: str = "AADS",
        anomaly_score: float = 0.0,
        metric_id: str = "default",
    ) -> str:
        """Insert structured decision trajectory into SQLite memory."""
        memory_id = f"mem-{uuid4().hex[:10]}"
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO agent_decisions (
                    memory_id, event_id, timestamp, action, tool, result,
                    success, latency_ms, event_type, detector, anomaly_score, metric_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    memory_id,
                    event_id,
                    str(timestamp),
                    action,
                    tool,
                    result,
                    1 if success else 0,
                    float(latency_ms),
                    event_type,
                    detector,
                    float(anomaly_score),
                    metric_id,
                ),
            )
            conn.commit()
        return memory_id

    def retrieve_similar_events(
        self,
        metric_id: Optional[str] = None,
        anomaly_score: Optional[float] = None,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """Retrieve recent or score-similar previous agent memory records."""
        with self._get_connection() as conn:
            if metric_id and metric_id != "default":
                query = """
                    SELECT * FROM agent_decisions
                    WHERE metric_id = ?
                    ORDER BY ABS(anomaly_score - ?) ASC, timestamp DESC
                    LIMIT ?
                """
                cursor = conn.execute(query, (metric_id, anomaly_score or 0.0, limit))
            else:
                query = """
                    SELECT * FROM agent_decisions
                    ORDER BY timestamp DESC
                    LIMIT ?
                """
                cursor = conn.execute(query, (limit,))

            rows = cursor.fetchall()

        results = []
        for r in rows:
            results.append({
                "memory_id": r["memory_id"],
                "event_id": r["event_id"],
                "timestamp": r["timestamp"],
                "action": r["action"],
                "tool": r["tool"],
                "result": r["result"],
                "success": bool(r["success"]),
                "latency_ms": r["latency_ms"],
                "event_type": r["event_type"],
                "detector": r["detector"],
                "anomaly_score": r["anomaly_score"],
                "metric_id": r["metric_id"],
            })
        return results

    def clear(self) -> None:
        """Clear memory table (used in tests)."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM agent_decisions")
            conn.commit()
