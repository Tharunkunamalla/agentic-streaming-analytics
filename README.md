# An Agentic AI Framework for Autonomous Streaming Data Analytics

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-black.svg)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5%2B-orange.svg)](https://spark.apache.org/)
[![LangGraph](https://img.shields.io/badge/LangGraph-Agentic%20Orchestration-green.svg)](https://github.com/langchain-ai/langgraph)
[![License](https://img.shields.io/badge/License-Academic%20Project-lightgrey.svg)]()

> **Course Project:** IOE413 Streaming Data Analytics Mini-Project  
> **Topic:** Autonomous Streaming Data Analytics with Event-Driven Agentic Orchestration

---

## 1. Executive Summary & Research Positioning

### 1.1 Project Objective
Modern cloud computing environments generate high-velocity telemetry and KPI time series where traditional fixed-threshold and purely statistical detectors struggle with non-stationary distributions, transient spikes, and concept drifts. While streaming anomaly detectors (e.g., AADS, xStream, HSTree, RRCF) offer millisecond-level throughput, they lack contextual reasoning to autonomously triage alerts, diagnose drift, and dynamically select optimal detection heuristics.

This project implements an **end-to-end, event-driven streaming analytics framework** that couples lightweight, high-throughput streaming anomaly detection with a **LangGraph-based agentic reasoning layer**.

### 1.2 Research Foundation & Academic Grounding
Our system is grounded in peer-reviewed literature:
1. **AADS (Autonomous Anomaly Detection for Streaming Data)**: *Knowledge-Based Systems*, 2024. Provides the online baseline streaming statistical detection model.
2. **StreamAD**: *A cloud platform metrics-oriented benchmark for unsupervised online anomaly detection*, 2023. Provides benchmark datasets and comparative streaming algorithms (xStream, HSTree, RRCF).
3. **LEMAD (LLM-Empowered Multi-Agent System for Anomaly Detection in Power Grid Services)**: 2025. Informs agentic decision workflows and domain contextualization.

### 1.3 Architectural Contribution & Boundaries
- **Controlled Orchestration**: We do *not* claim to have invented agentic anomaly detection or to be the first to combine LLMs with time-series analysis. Our contribution is a **controlled, event-driven orchestration architecture** operating over a Kafka boundary.
- **Hierarchical First-Stage Filter**: The LLM agent is **never** invoked on raw streaming records. Spark Structured Streaming and the AADS streaming baseline process every continuous metric in sub-millisecond time.
- **Event-Driven Agentic Trigger**: Only candidate anomaly events exceeding statistical thresholds trigger the asynchronous LangGraph agent.
- **Constrained Predefined Toolset**: The agent operates solely through strictly defined analytical tools (statistics, drift testing, detector comparison, episodic memory). It **never executes arbitrary code or shell commands**.

---

## 2. End-to-End Architecture

```
                  STREAMAD DATASET
                         │
                         ▼
                 Python Replay
                    Producer
                         │
                         ▼
                ┌────────────────┐
                │     KAFKA      │
                │  raw-metrics   │
                └───────┬────────┘
                        │
                        ▼
              ┌───────────────────┐
              │ Spark Structured   │
              │ Streaming          │
              └─────────┬─────────┘
                        │
                        ▼
                 ┌────────────┐
                 │    AADS    │
                 │  Baseline  │
                 └─────┬──────┘
                       │
              ┌────────┴────────┐
              │                 │
            NORMAL           ANOMALY
                                │
                                ▼
                       Context Builder
                                │
                                ▼
                       ┌────────────────┐
                       │   LangGraph    │
                       │     Agent      │
                       └───────┬────────┘
                               │
                 ┌─────────────┼─────────────┐
                 ▼             ▼             ▼
             Statistics      Drift       Detector
                Tool          Tool       Comparison
                 │             │             │
                 └─────────────┼─────────────┘
                               ▼
                           Validator
                               │
                               ▼
                            Memory
                               │
                               ▼
                       Final Decision
                               │
                               ▼
                           Database
                               │
                               ▼
                            Grafana
```

---

## 3. Predefined Agent Actions & Analytical Tools

### 3.1 Primary Actions
- `NO_ACTION`: Disregard transient noise or false alarm.
- `INVESTIGATE`: Request window context expansion and anomaly verification.
- `CHECK_DRIFT`: Trigger two-sample Kolmogorov-Smirnov / Page-Hinkley test on stream baseline.
- `COMPARE_DETECTORS`: Run multi-detector benchmark against window (AADS, xStream, HSTree, RRCF).
- `RUN_ALTERNATIVE_DETECTOR`: Switch streaming pipeline to recommended detector.
- `REQUEST_DEEP_ANALYSIS`: Escalate persistent systemic anomalies for operator intervention.

### 3.2 Controlled Analytical Tools
1. `calculate_statistics`: Window mean, standard deviation, kurtosis, skewness, interquartile range.
2. `check_drift`: Non-parametric drift and change-point testing.
3. `run_xstream`: StreamAD random projection detector.
4. `run_hstree`: StreamAD Half-Space Trees detector.
5. `run_rrcf`: Robust Random Cut Forest detector.
6. `compare_detectors`: Ensemble scoring and consensus evaluation.
7. `retrieve_similar_events`: Vector similarity lookup across historical anomaly incidents.
8. `store_decision`: Persist validated decision to episodic memory and relational storage.

---

## 4. Repository Layout

```
├── .env.example              # Environment variables template
├── .gitignore                # Git ignore patterns
├── pyproject.toml            # Project packaging & pytest/ruff config
├── requirements.txt          # Production dependencies
├── docker/
│   └── docker-compose.yml    # Kafka (KRaft), Postgres, Grafana, Kafka UI
├── scripts/
│   └── check_env.py          # Phase 1 verification script
├── src/
│   ├── __init__.py
│   ├── config/               # Pydantic BaseSettings & configuration
│   ├── schemas/              # Pydantic data contracts (Metric, Anomaly, Agent, etc.)
│   ├── utils/                # Structured logging (JSON/Text), seed utilities
│   ├── producer/             # Stream replay producer (Phase 2)
│   ├── streaming/            # Spark Structured Streaming pipeline (Phase 3)
│   ├── detectors/            # AADS & StreamAD detectors (Phases 4 & 6)
│   ├── agent/                # LangGraph state machine, tools & memory (Phases 8-10)
│   ├── storage/              # Database persistence & vector memory (Phase 11)
│   └── evaluation/           # Detection, Streaming & Agent metrics (Phases 5 & 12)
├── tests/
│   ├── conftest.py           # Pytest global fixtures
│   ├── unit/                 # Unit test suite
│   │   ├── test_config.py
│   │   ├── test_schemas.py
│   │   └── test_logger.py
│   └── integration/          # End-to-end integration tests
└── data/
    ├── raw/                  # Datasets (AIOPS_KPI, SMD)
    └── processed/            # Processed artifacts
```

---

## 5. Development Phases

| Phase | Description | Status |
|---|---|---|
| **Phase 1** | Repository structure, environment, schemas, configuration & logging | **Completed** |
| **Phase 2** | Kafka infrastructure & dataset replay producer | Pending |
| **Phase 3** | Spark Structured Streaming pipeline | Pending |
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

## 6. Phase 1 Verification & Testing

### 6.1 Running the Environment Verification Script
```bash
python scripts/check_env.py
```

### 6.2 Running Unit Tests
```bash
pytest tests/unit -v
```
