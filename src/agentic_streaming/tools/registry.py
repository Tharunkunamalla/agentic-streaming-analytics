"""Allowlisted Tool Registry for LangGraph Agent.

Enforces strict input typing, output typing, execution latency logging,
error handling, and safe execution for all 8 agent tools.
"""

from dataclasses import dataclass
import logging
import time
from typing import Any, Callable, Dict, Optional, Type

from pydantic import BaseModel, ValidationError

from src.agentic_streaming.tools.detectors_tools import (
    compare_detectors_tool,
    run_hstree_tool,
    run_rrcf_tool,
    run_xstream_tool,
)
from src.agentic_streaming.tools.drift import check_drift_tool
from src.agentic_streaming.tools.memory_tools import (
    retrieve_similar_events_tool,
    store_decision_tool,
)
from src.agentic_streaming.tools.schemas import (
    CalculateStatisticsInput,
    CalculateStatisticsOutput,
    CheckDriftInput,
    CheckDriftOutput,
    CompareDetectorsInput,
    CompareDetectorsOutput,
    RetrieveSimilarEventsInput,
    RetrieveSimilarEventsOutput,
    SingleDetectorInput,
    SingleDetectorOutput,
    StoreDecisionInput,
    StoreDecisionOutput,
)
from src.agentic_streaming.tools.statistics import calculate_statistics_tool

logger = logging.getLogger("agentic_streaming.tools.registry")


@dataclass
class RegisteredTool:
    name: str
    description: str
    input_schema: Type[BaseModel]
    output_schema: Type[BaseModel]
    func: Callable[[Any], BaseModel]


class ToolRegistry:
    """Allowlisted registry managing tool lookup, schema validation, and execution logging."""

    def __init__(self) -> None:
        self._tools: Dict[str, RegisteredTool] = {}
        self._register_default_tools()

    def register(
        self,
        name: str,
        description: str,
        input_schema: Type[BaseModel],
        output_schema: Type[BaseModel],
        func: Callable[[Any], BaseModel],
    ) -> None:
        """Register a new allowlisted tool."""
        self._tools[name] = RegisteredTool(
            name=name,
            description=description,
            input_schema=input_schema,
            output_schema=output_schema,
            func=func,
        )

    def _register_default_tools(self) -> None:
        """Register the required 8 controlled agent tools."""
        self.register(
            name="calculate_statistics",
            description="Calculate summary statistics (mean, std, min, max, z-score) for metric values",
            input_schema=CalculateStatisticsInput,
            output_schema=CalculateStatisticsOutput,
            func=calculate_statistics_tool,
        )
        self.register(
            name="check_drift",
            description="Perform KS-test and PSI statistical distribution drift analysis",
            input_schema=CheckDriftInput,
            output_schema=CheckDriftOutput,
            func=check_drift_tool,
        )
        self.register(
            name="run_xstream",
            description="Run xStream anomaly detector on current metric and window history",
            input_schema=SingleDetectorInput,
            output_schema=SingleDetectorOutput,
            func=run_xstream_tool,
        )
        self.register(
            name="run_hstree",
            description="Run HSTree anomaly detector on current metric and window history",
            input_schema=SingleDetectorInput,
            output_schema=SingleDetectorOutput,
            func=run_hstree_tool,
        )
        self.register(
            name="run_rrcf",
            description="Run Robust Random Cut Forest anomaly detector on metric and window history",
            input_schema=SingleDetectorInput,
            output_schema=SingleDetectorOutput,
            func=run_rrcf_tool,
        )
        self.register(
            name="compare_detectors",
            description="Run all streaming detectors (AADS, HSTree, xStream, RRCF) and compute consensus",
            input_schema=CompareDetectorsInput,
            output_schema=CompareDetectorsOutput,
            func=compare_detectors_tool,
        )
        self.register(
            name="retrieve_similar_events",
            description="Retrieve similar previous anomaly events and agent decisions from SQLite memory",
            input_schema=RetrieveSimilarEventsInput,
            output_schema=RetrieveSimilarEventsOutput,
            func=retrieve_similar_events_tool,
        )
        self.register(
            name="store_decision",
            description="Store agent decision trajectory permanently into SQLite memory",
            input_schema=StoreDecisionInput,
            output_schema=StoreDecisionOutput,
            func=store_decision_tool,
        )

    def list_tools(self) -> Dict[str, str]:
        """Return dict of available tool names and descriptions."""
        return {name: tool.description for name, tool in self._tools.items()}

    def get_tool(self, name: str) -> RegisteredTool:
        """Fetch tool definition or raise error if not allowlisted."""
        if name not in self._tools:
            raise KeyError(f"Tool '{name}' is not registered in the allowlisted tool registry.")
        return self._tools[name]

    def execute(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Validate input schema, execute tool safely, measure latency, and return JSON dict output."""
        if tool_name not in self._tools:
            return {
                "success": False,
                "error": f"Tool '{tool_name}' is not allowlisted in registry.",
                "latency_ms": 0.0,
            }

        tool = self._tools[tool_name]
        t0 = time.perf_counter()

        try:
            validated_input = tool.input_schema(**arguments)
            raw_output = tool.func(validated_input)
            
            if isinstance(raw_output, BaseModel):
                validated_output = tool.output_schema(**raw_output.model_dump())
                result_data = validated_output.model_dump()
            else:
                result_data = raw_output

            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            logger.info("Tool '%s' executed successfully in %.2f ms", tool_name, elapsed_ms)

            return {
                "success": True,
                "tool": tool_name,
                "result": result_data,
                "latency_ms": round(elapsed_ms, 3),
            }

        except ValidationError as val_err:
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            logger.warning("Tool '%s' input schema validation failed: %s", tool_name, str(val_err))
            return {
                "success": False,
                "tool": tool_name,
                "error": f"Schema validation error: {str(val_err)}",
                "latency_ms": round(elapsed_ms, 3),
            }
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            logger.error("Tool '%s' failed during execution: %s", tool_name, str(exc), exc_info=True)
            return {
                "success": False,
                "tool": tool_name,
                "error": f"Execution failure: {str(exc)}",
                "latency_ms": round(elapsed_ms, 3),
            }
