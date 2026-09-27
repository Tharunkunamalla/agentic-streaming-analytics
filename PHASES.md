# Project Roadmap & Implementation Phases

Track the development lifecycle for **An Agentic AI Framework for Autonomous Streaming Data Analytics** across the 13 planned engineering and research phases.

---

## Progress Overview

| Phase | Description | Status |
|---|---|---|
| **Phase 1** | Repository structure, environment, schemas, configuration & logging | **Completed** |
| **Phase 2A** | Dataset acquisition & validation pipeline (AIOPS_KPI, manifest, sample) | **Completed** |
| **Phase 2B** | Kafka infrastructure & dataset replay producer | **Completed** |
| **Phase 3** | Spark Structured Streaming pipeline | **Completed** |
| **Phase 4** | AADS streaming anomaly detection baseline | **Completed** |
| **Phase 5** | Baseline evaluation (Precision, Recall, F1, Latency) | **Completed** |
| **Phase 6** | StreamAD comparison detectors (xStream, HSTree, RRCF) | **Completed** |
| **Phase 7** | Anomaly event Kafka pipeline & context builder | **Completed** |
| **Phase 8** | LangGraph agent state machine & decision node | **Completed** |
| **Phase 9** | Controlled agent tools & allowlisted registry | **Completed** |
| **Phase 10** | Simple durable SQLite memory & event retrieval | **Completed** |
| **Phase 11** | Autonomous analytical & detector selection policy | **Completed** |
| **Phase 12** | Relational database storage (events, detections, decisions, tools) | **Completed** |
| **Phase 13** | Grafana real-time monitoring dashboard & telemetry | **Completed** |
| **Phase 14** | Ablation study configs, stream-rate benchmark & decision evaluator | **Completed** |
| **Phase 15** | Comparative results generation & final report artifacts | Pending |

---

## Detailed Phase Breakdown

### Phase 1: Repository Structure & Environment
- [x] Initialized directory layout (`src/agentic_streaming`, `config`, `docker`, `tests`, `docs`, `experiments`).
- [x] Pydantic configuration models with `.env` loading and environment overrides.
- [x] Structured JSON / Text logging with ISO timestamps and context fields.
- [x] Pydantic data schemas: `MetricRecord`, `AnomalyEvent`, `AgentDecision`, `DriftReport`, `DetectorScore`, evaluation metrics.
- [x] Docker Compose stack definition (Kafka KRaft, Kafka UI, PostgreSQL, Grafana).
- [x] Deterministic random seed utilities.
- [x] Unit test suite (`tests/unit/`) with 100% pass rate.

### Phase 2A: Dataset Acquisition & Validation Pipeline
- [x] Automated benchmark downloader script (`scripts/download_data.py`).
- [x] Acquired official AIOPS_KPI dataset from StreamAD / NetMan benchmark repository (3,004,066 rows across 29 KPI streams).
- [x] Rigorous validation pipeline (`scripts/prepare_aiops_kpi.py`):
  - Column schema verification (`timestamp`, `kpi_id`, `value`, `ground_truth`).
  - Strict monotonic timestamp ordering per KPI stream.
  - Zero synthetic labels fabricated; exact benchmark ground truth preserved (79,554 anomalies / 2.648%).
  - Deduplication and numeric casting.
- [x] Generated dataset manifest (`data/processed/dataset_manifest.json`) and integration test sample (`data/sample/aiops_kpi_sample.csv`).
- [x] Unit tests for dataset validation and schema conformance (`tests/unit/test_dataset.py`).

### Phase 2B: Kafka Streaming & Dataset Replay Producer
- [x] Docker Compose KRaft Kafka stack with healthchecks and topic provisioning.
- [x] Created all required Kafka topics: `raw-metrics`, `processed-metrics`, `anomaly-events`, `agent-decisions`, `analytics-results`.
- [x] Built `StreamingReplayProducer` (`src/agentic_streaming/kafka/producer.py`):
  - Row-by-row time-series streaming.
  - Configurable rates (`--rate 1, 10, 50, 100`), `--limit`, and `--loop`.
  - Enriched JSON payloads with `event_id`, `ingestion_timestamp`, `ground_truth`, and `source`.
- [x] Built `StreamingConsumer` (`src/agentic_streaming/kafka/consumer.py`) and verification script (`scripts/verify_kafka_stream.py`).
- [x] Verified Milestone: Replayed 100 events into Kafka (`raw-metrics`) with 0 errors and 100% acknowledgments.
- [x] Unit and integration tests passing (`tests/unit/test_kafka_producer.py`, `tests/integration/test_kafka_stream.py`).

### Phase 3: Spark Structured Streaming Pipeline
- [x] Implement Spark Structured Streaming pipeline (`src/agentic_streaming/streaming/pipeline.py`).
- [x] Parse JSON schema, convert timestamps to event-time, and safely filter malformed records.
- [x] Perform configurable event-time sliding windows.
- [x] Calculate rolling statistics (rolling_mean, rolling_std, rolling_min, rolling_max, rolling_count).
- [x] Checkpointing support (`data/checkpoints/spark`).
- [x] Emit enriched processed metrics to Kafka `processed-metrics` topic.
- [x] Real-time health/status telemetry (records processed, window, throughput, malformed count).
- [x] End-to-end integration test: Kafka producer -> `raw-metrics` -> Spark streaming -> `processed-metrics` (`tests/integration/test_spark_streaming_e2e.py`).
- [x] Unit tests for rolling window statistics and error handling (`tests/unit/test_streaming_pipeline.py`).

### Phase 4: AADS Streaming Baseline Detector
- [x] Faithful 3-stage implementation of AADS (*Knowledge-Based Systems*, 2024):
  - Stage 1: Data-density-based identification of potentially anomalous samples (`src/agentic_streaming/aads/density.py`).
  - Stage 2: Online clustering using evolving autonomous data partitioning approach (`src/agentic_streaming/aads/clustering.py`).
  - Stage 3: Identification of true anomalies from minor clusters (`src/agentic_streaming/aads/detector.py`).
- [x] Comprehensive architectural assumptions and formula documentation (`docs/aads_assumptions.md`).
- [x] Fully configurable hyperparameter space (`src/agentic_streaming/aads/config.py`).
- [x] Offline streaming evaluator and metrics engine (`src/agentic_streaming/aads/evaluator.py`, `scripts/run_aads_offline.py`).
- [x] Offline baseline verification on AIOPS_KPI 5,000-sample integration slice:
  - Throughput: 9,153+ events/sec
  - Average latency: 0.035 ms / event (sub-millisecond streaming classification)
  - Confusion matrix: TP=21, FP=137, TN=4787, FN=55 (saved in `experiments/baseline/aads_offline_evaluation.json`).
- [x] Comprehensive unit tests for density, micro-clustering, detector, and evaluator (`tests/unit/test_aads_*.py`).
- [x] All 37 unit and integration tests passing cleanly.

### Phase 5: Baseline Evaluation
- [x] Comprehensive empirical benchmark of standalone AADS baseline on 50,000 real cloud KPI time-series events (`data/processed/aiops_kpi_clean.csv`):
  - **Dataset:** AIOPS_KPI (StreamAD benchmark source, 50,000 evaluated records, 341 actual ground truth anomalies).
  - **Execution Time:** 8.72 seconds.
  - **Streaming Throughput:** 5,732.4 events/sec.
  - **Mean Classification Latency:** 0.0523 ms/event.
  - **Precision:** 17.36% (TP = 75, FP = 357).
  - **Recall:** 21.99% (FN = 266).
  - **F1-Score:** 19.40%.
  - **False Positive Rate (FPR):** 0.720% (TN = 49,302).
- [x] Baseline results serialized to `experiments/baseline/aads_offline_evaluation.json`.
- [x] Analysis: Confirmed the critical motivation for the Agentic AI orchestration layer — standalone AADS provides ultra-fast first-stage stream filtering (99.14% stream reduction, 0.05 ms latency), but yields 357 false alerts needing second-stage autonomous agent validation.

### Phase 6: StreamAD Comparison Detectors
- [x] Abstract base streaming detector contract (`src/agentic_streaming/detectors/base.py`).
- [x] Implemented Half-Space Trees (HSTree) online ensemble (`src/agentic_streaming/detectors/hstree.py`).
- [x] Implemented xStream multi-projection density estimator (`src/agentic_streaming/detectors/xstream.py`).
- [x] Implemented Robust Random Cut Forest (RRCF) with Collusive Displacement (`src/agentic_streaming/detectors/rrcf_detector.py`).
- [x] Multi-detector comparative streaming benchmark runner (`scripts/run_detector_benchmark.py`).
- [x] Comprehensive comparative evaluation on 5,000 identical cloud KPI streaming events:
  | Detector | Precision | Recall | F1-Score | FPR | Throughput | Latency |
  | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
  | **AADS (Baseline)** | **13.29%** | 27.63% | **17.95%** | **2.780%** | 14,121 eps | **0.0232 ms** |
  | **HSTree** | 6.52% | 19.74% | 9.80% | 4.366% | **22,402 eps** | 0.0444 ms |
  | **xStream** | 0.00% | 0.00% | 0.00% | 0.000% | 3,532 eps | 0.2826 ms |
  | **RRCF** | 1.42% | **86.84%** | 2.79% | 93.217% | 710 eps | 1.4049 ms |
- [x] Comparative benchmark results saved to `experiments/baseline/detector_comparison_benchmark.json`.
- [x] Complete unit test suite verifying all streaming detectors (`tests/unit/test_detectors.py`). All 41 tests passing.

### Phase 7: Anomaly Event Pipeline & Context Builder
- [x] Standardized `AnomalyEvent` payload schema (`src/schemas/anomaly.py`) storing event_id, timestamp, dataset, features, anomaly_score, detector, recent_window_summary, anomaly_frequency, and recent_detector_metrics.
- [x] Connected Spark streaming pipeline to AADS baseline (`src/agentic_streaming/streaming/pipeline.py`):
  - Streamed events: `Kafka raw-metrics -> Spark -> AADS`.
  - Nominal events are filtered and NEVER trigger the anomaly topic or LLM agent.
  - Flagged anomaly candidates emit structured `AnomalyEvent` payloads to Kafka `anomaly-events` topic.
- [x] End-to-end integration test (`tests/integration/test_anomaly_event_pipeline.py`) proving real Kafka data flow (`dataset -> Kafka -> Spark -> AADS -> anomaly-events`).
- [x] Unit test verifying pipeline anomaly integration (`tests/unit/test_streaming_pipeline_anomaly.py`).

### Phase 8: LangGraph Agent Core
- [x] Defined strongly typed AgentState (`src/agentic_streaming/agent/state.py` and `agent/state.py`) and closed action enum `AgentAction` (`NO_ACTION`, `INVESTIGATE`, `CHECK_DRIFT`, `COMPARE_DETECTORS`, `RUN_ALTERNATIVE_DETECTOR`, `REQUEST_DEEP_ANALYSIS`).
- [x] Structured system prompts and JSON schema enforcement (`src/agentic_streaming/agent/prompts.py` and `agent/prompts.py`).
- [x] Implemented AgentPlanner node (`src/agentic_streaming/agent/planner.py` and `agent/planner.py`) for contextual profiling and reasoning.
- [x] Implemented AgentValidator node (`src/agentic_streaming/agent/validator.py` and `agent/validator.py`) enforcing Pydantic validation, single retry on invalid LLM JSON, and deterministic severity-based fallback.
- [x] Implemented StreamingAgentWorkflow state machine (`src/agentic_streaming/agent/graph.py` and `agent/graph.py`) connecting:
  `START -> Profile -> Plan -> Select Action -> Execute Tool -> Validate -> Store Memory -> END`.
- [x] Strict security enforcement: 0 arbitrary code execution, 0 shell tools, 0 raw credentials.
- [x] Complete unit test suite (`tests/unit/test_agent.py`) verifying state machine, validation retries, fallback mechanics, and action space constraints. All tests passing.

### Phase 9: Controlled Agent Tools & Allowlisted Registry
- [x] Implemented all 8 required agent tools with typed Pydantic input/output schemas:
  1. `calculate_statistics`: Mean, std, min, max, median, p95, z-score.
  2. `check_drift`: KS-test 2-sample p-value and Population Stability Index (PSI).
  3. `run_xstream`: Multi-projection online stream detector.
  4. `run_hstree`: Streaming isolation tree ensemble.
  5. `run_rrcf`: Robust Random Cut Forest anomaly evaluation.
  6. `compare_detectors`: Parallel evaluation across all 4 detectors with agreement ratio.
  7. `retrieve_similar_events`: Episodic memory lookup from SQLite.
  8. `store_decision`: Permanent decision trajectory recording into SQLite memory.
- [x] Built allowlisted `ToolRegistry` (`src/agentic_streaming/tools/registry.py`) measuring execution latency, validating schemas, handling execution errors safely, and preventing arbitrary code execution.
- [x] Unit test suite (`tests/unit/test_tools.py`) verifying tool execution, error handling, and schema validation.

### Phase 10: Simple Durable Memory Store
- [x] Simple, durable SQLite memory manager (`src/agentic_streaming/memory/sqlite_memory.py`) saving experiment metadata to `data/memory.db`.
- [x] Every memory record includes required fields: `event_id`, `timestamp`, `action`, `tool`, `result`, `success`, `latency_ms`, `event_type`, `detector`.
- [x] Implemented `store_decision()` and `retrieve_similar_events()` for simple episodic memory lookup.
- [x] Unit test suite (`tests/unit/test_memory.py`) proving stored events can be retrieved. All 49 test cases passing cleanly.

### Phase 11: Autonomous Detector Selection Policy
- [x] Implemented deterministic detector adaptation engine (`src/agentic_streaming/detectors/detector_selector.py`).
- [x] Closed workflow execution path:
  1. AADS flags anomaly event.
  2. Compact context payload built (`current_event`, `recent_window`, `anomaly`, `detector`).
  3. Memory lookup for similar historical trajectories.
  4. Agent selects analysis action (`CHECK_DRIFT` or `COMPARE_DETECTORS`).
  5. Tool executed via allowlisted `ToolRegistry`.
  6. Results validated.
  7. If concept drift indicated (PSI > 0.20): multi-detector benchmark executed.
  8. Empirical benchmark results evaluated using deterministic policy rule to select best detector for next window (LLM requests analysis, empirical experiment decides detector).
  9. Decision and adaptation record logged.
  10. Streaming pipeline continues without source code modification.

### Phase 12: Relational Database Storage
- [x] Implemented `RelationalStorageManager` (`src/agentic_streaming/storage/relational_storage.py`) writing to SQLite database (`data/streaming_analytics.db`).
- [x] Relational schema implemented:
  - `events` (`event_id`, `timestamp`, `metric_id`, `value`, `features_json`)
  - `detections` (`detection_id`, `event_id`, `detector`, `score`, `prediction`, `latency_ms`, `timestamp`)
  - `agent_decisions` (`decision_id`, `event_id`, `action`, `reason`, `success`, `latency_ms`, `active_detector`, `timestamp`)
  - `tool_executions` (`execution_id`, `event_id`, `tool`, `start_time`, `end_time`, `success`, `result_json`, `latency_ms`)
  - `experiments` (`experiment_id`, `name`, `start_time`, `end_time`, `config_json`, `status`)
  - `metrics` (`metric_id`, `experiment_id`, `timestamp`, `precision`, `recall`, `f1`, `fpr`, `latency_ms`, `throughput_eps`, `active_detector`)
- [x] Unit test suite (`tests/unit/test_phase11_phase12.py`) verifying detector adaptation policy, relational table insertions, and workflow integration. All tests passing cleanly.

### Phase 13: Grafana Real-Time Monitoring Dashboard
- [x] Provisioned Grafana SQLite datasource (`docker/grafana/provisioning/datasources/sqlite.yaml`).
- [x] Created automatic dashboard provider (`docker/grafana/provisioning/dashboards/dashboards.yaml`).
- [x] Created agentic streaming analytics Grafana dashboard layout (`docker/grafana/provisioning/dashboards/agentic_streaming_dashboard.json`):
  - Stat cards: Events/sec, Anomalies, AADS F1, Agent decisions, Avg agent latency.
  - Time-series panel: Live metric stream with anomaly markers.
  - Table panel: Recent agent actions, tool results, and distribution change flags.
  - Current detector status card.

### Phase 14: Experiment Framework & Ablation Configurations
- [x] Created experiment configuration matrix (`experiments/configs/`):
  - `baseline_aads.yaml` (AADS standalone baseline)
  - `streamad_comparison.yaml` (HSTree, xStream, RRCF benchmark)
  - `agent_no_memory.yaml` (AADS + Agent without episodic memory)
  - `agent_with_memory.yaml` (AADS + Agent + SQLite memory)
  - `full_proposed.yaml` (AADS + Agent + Memory + Adaptation Policy)
- [x] Stream-rate benchmark suite configured (`STREAM_RATE=1, 10, 50, 100`).
- [x] Implemented scenario-based decision evaluation engine (`src/agentic_streaming/metrics/agent_evaluator.py`) measuring decision accuracy against ground truth scenarios (isolated strong anomaly, repeated anomaly, uncertain behavior, distribution change, no evidence).

### Phase 15: Comparative Results Generation & Report Artifacts
- [ ] Execute full ablation study matrix and populate empirical results table template:
  - **Detection Performance:** Precision, Recall, F1, FPR.
  - **Streaming System Performance:** Throughput, Classification Latency, Memory Usage.
  - **Agent Decision Metrics:** Decision accuracy, Decision success, Avg decision latency, Unnecessary tool calls, LLM calls, Cost.
  - **Adaptation Performance:** Drift detection accuracy, Detector switches, Recovery time.
