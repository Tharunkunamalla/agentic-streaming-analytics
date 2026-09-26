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
| **Phase 4** | AADS streaming anomaly detection baseline | Pending |
| **Phase 5** | Baseline evaluation (Precision, Recall, F1, Latency) | Pending |
| **Phase 6** | StreamAD comparison detectors (xStream, HSTree, RRCF) | Pending |
| **Phase 7** | Anomaly event Kafka pipeline & context builder | Pending |
| **Phase 8** | LangGraph agent state machine & decision node | Pending |
| **Phase 9** | Agent analytical tools & vector memory | Pending |
| **Phase 10** | Autonomous detector/analysis selection logic | Pending |
| **Phase 11** | Database storage & Grafana dashboard provisioning | Pending |
| **Phase 12** | Comprehensive comparative experiments | Pending |
| **Phase 13** | Report artifacts, figures & demonstration package | Pending |

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
- [ ] Faithful implementation of AADS (*Knowledge-Based Systems*, 2024).
- [ ] Sliding statistical window with exponential decay weighting.
- [ ] Streaming distance calculation and adaptive thresholding.
- [ ] Sub-millisecond record classification (Normal vs Anomaly candidate).

### Phase 5: Baseline Evaluation
- [ ] Benchmark AADS against dataset ground truth labels.
- [ ] Calculate Precision, Recall, F1-score, and False Positive Rate (FPR).
- [ ] Measure throughput (events/sec), detection latency, and memory profile.

### Phase 6: StreamAD Comparison Detectors
- [ ] Implement xStream (Random Projection Anomaly Detector).
- [ ] Implement Half-Space Trees (HSTree).
- [ ] Implement Robust Random Cut Forest (RRCF).
- [ ] Benchmark comparison algorithms on identical stream data.

### Phase 7: Anomaly Event Pipeline & Context Builder
- [ ] Publish detected anomaly events to Kafka `anomaly-events` topic.
- [ ] Build contextual window buffers (pre/post anomaly snapshots, summary stats).
- [ ] Asynchronous event consumer boundary isolating streaming from agentic reasoning.

### Phase 8: LangGraph Agent Core
- [ ] Define LangGraph agent state graph and decision nodes.
- [ ] System prompt design enforcing predefined action space:
  - `NO_ACTION`, `INVESTIGATE`, `CHECK_DRIFT`, `COMPARE_DETECTORS`, `RUN_ALTERNATIVE_DETECTOR`, `REQUEST_DEEP_ANALYSIS`.
- [ ] Deterministic LLM interaction with timeout and retry handling.

### Phase 9: Agent Tools & Episodic Memory
- [ ] Controlled tools: `calculate_statistics`, `check_drift`, `run_xstream`, `run_hstree`, `run_rrcf`, `compare_detectors`.
- [ ] Vector memory integration (ChromaDB) for historical anomaly retrieval (`retrieve_similar_events`, `store_decision`).
- [ ] Decision output validation preventing hallucinated actions or parameters.

### Phase 10: Autonomous Detector & Analysis Selection
- [ ] Context-aware dynamic detector switching logic.
- [ ] Regime change and concept drift adaptation protocol.
- [ ] Feedback loop between agent decisions and streaming pipeline.

### Phase 11: Persistent Storage & Grafana Dashboards
- [ ] PostgreSQL / SQLite relational tables for anomaly logs, decisions, and tool traces.
- [ ] Provision Grafana data sources and real-time streaming dashboards.
- [ ] Visual telemetry: Stream metrics, anomaly markers, agent actions, and latency metrics.

### Phase 12: Empirical Experiments & Comparative Evaluation
- [ ] Experiment 1: AADS Baseline vs. AADS + Agentic Layer.
- [ ] Experiment 2: Multi-detector performance under synthetic & real concept drifts.
- [ ] Experiment 3: Agent decision accuracy, unnecessary tool call rates, and latency overhead.

### Phase 13: Report Artifacts & Demonstration Package
- [ ] Generate figures, confusion matrices, and ablation tables.
- [ ] Finalize technical documentation and course mini-project report.
- [ ] End-to-end replay demo script for presentation.
