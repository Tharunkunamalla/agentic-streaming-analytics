"""Controlled agent tools for retrieving similar events and storing decisions in SQLite memory."""

from typing import Optional
from src.agentic_streaming.memory.sqlite_memory import SQLiteMemoryManager
from src.agentic_streaming.tools.schemas import (
    RetrieveSimilarEventsInput,
    RetrieveSimilarEventsOutput,
    StoreDecisionInput,
    StoreDecisionOutput,
)

_DEFAULT_MEMORY_MANAGER: Optional[SQLiteMemoryManager] = None


def get_memory_manager() -> SQLiteMemoryManager:
    """Singleton getter for the SQLiteMemoryManager."""
    global _DEFAULT_MEMORY_MANAGER
    if _DEFAULT_MEMORY_MANAGER is None:
        _DEFAULT_MEMORY_MANAGER = SQLiteMemoryManager()
    return _DEFAULT_MEMORY_MANAGER


def retrieve_similar_events_tool(inp: RetrieveSimilarEventsInput) -> RetrieveSimilarEventsOutput:
    """Retrieve similar previous anomaly events and decisions from memory."""
    mem = get_memory_manager()
    results = mem.retrieve_similar_events(
        metric_id=inp.metric_id,
        anomaly_score=inp.anomaly_score,
        limit=inp.limit,
    )
    return RetrieveSimilarEventsOutput(similar_events=results, count=len(results))


def store_decision_tool(inp: StoreDecisionInput) -> StoreDecisionOutput:
    """Store agent decision trajectory permanently into SQLite memory."""
    mem = get_memory_manager()
    mem_id = mem.store_decision(
        event_id=inp.event_id,
        timestamp=inp.timestamp,
        action=inp.action,
        tool=inp.tool,
        result=inp.result,
        success=inp.success,
        latency_ms=inp.latency_ms,
        event_type=inp.event_type,
        detector=inp.detector,
    )
    return StoreDecisionOutput(memory_id=mem_id, saved=True)
