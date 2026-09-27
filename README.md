# Agentic Streaming Analytics Framework

An end-to-end, event-driven streaming analytics pipeline that integrates high-throughput streaming anomaly detection with autonomous, agentic reasoning. Built for cloud platform metrics (AIOPS_KPI, SMD) using Apache Kafka, Apache Spark Structured Streaming, AADS, and LangGraph.

---

## Overview

High-velocity cloud telemetry streams require both sub-millisecond detection throughput and intelligent, context-aware triage. Traditional streaming anomaly detectors identify statistical outliers rapidly, but cannot autonomously diagnose concept drifts, evaluate multi-detector consensus, or reference historical operational incidents.

This project implements a **two-stage hierarchical framework**:
1. **Streaming Detection Layer (First Stage):** Apache Spark Structured Streaming and the AADS (*Autonomous Anomaly Detection for Streaming Data*) baseline process continuous time-series metrics in sub-millisecond real time.
2. **Agentic Orchestration Layer (Second Stage):** Only verified anomaly candidates are published to an `anomaly-events` Kafka topic. A LangGraph-based agent consumes these events and executes structured analytical tools (drift detection, multi-detector benchmarking, and episodic memory lookup) to reach a validated operational decision.

```
                         ┌─────────────────────┐
                         │      DATASET        │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   Kafka Producer    │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │       KAFKA         │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │       SPARK         │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │       AADS          │
                         └──────────┬──────────┘
                                    │
                              anomaly?
                              /      \
                            no        yes
                            │          │
                            │          ▼
                            │     ┌───────────┐
                            │     │   AGENT   │
                            │     └─────┬─────┘
                            │           │
                            │       ┌───┼───┐
                            │       ▼   ▼   ▼
                            │      Tools/Memory
                            │           │
                            │           ▼
                            │       Decision
                            │
                            └──────────┬──────────
                                       │
                                       ▼
                              ┌────────────────┐
                              │    DATABASE    │
                              └───────┬────────┘
                                      │
                        ┌─────────────┴─────────────┐
                        ▼                           ▼
                 ┌─────────────┐             ┌─────────────┐
                 │   GRAFANA   │             │ EXPERIMENTS │
                 │ LIVE VIEW   │             │ Python      │
                 └─────────────┘             └─────────────┘
                        │                           │
                        ▼                           ▼
                   DEMO UI                    REPORT TABLES
```

---

## Core Capabilities

- **Decoupled Event Boundary:** The streaming engine remains isolated from LLM response latencies. High-velocity stream ingestion is never blocked by agentic reasoning.
- **Controlled Analytical Toolset:** The agent chooses from predefined deterministic tools (`calculate_statistics`, `check_drift`, `compare_detectors`, `retrieve_similar_events`, `store_decision`). It does not execute arbitrary code or shell commands.
- **Adaptive Detector Switching:** Diagnoses concept drift via statistical tests (e.g., Kolmogorov-Smirnov) and benchmarks alternative StreamAD-compatible algorithms (xStream, HSTree, RRCF) in real time.
- **Episodic Vector Memory:** Uses vector similarity to retrieve similar past anomalies and their resolutions, improving decision consistency over time.
- **Full-Stack Observability:** Persists telemetry, anomaly events, tool execution traces, and agent decisions to PostgreSQL/SQLite, surfaced in real-time Grafana dashboards.

---

## Research Foundations

1. **AADS**: *Autonomous Anomaly Detection for Streaming Data*, Knowledge-Based Systems, 2024.  
   Provides the core online baseline streaming anomaly detection algorithm.
2. **StreamAD**: *A cloud platform metrics-oriented benchmark for unsupervised online anomaly detection*, 2023.  
   Supplies real-world cloud benchmark datasets (AIOPS_KPI, SMD) and comparative detector implementations (xStream, HSTree, RRCF).
3. **LEMAD**: *LLM-Empowered Multi-Agent System for Anomaly Detection in Power Grid Services*, 2025.  
   Informs agentic decision workflows and context formulation for streaming time series.

---

## Repository Structure

```
agentic-streaming-analytics/
├── README.md                  # Project overview and documentation
├── PHASES.md                  # Project development roadmap & progress tracking
├── LICENSE                    # MIT License
├── .gitignore                 # Git ignore rules
├── .env.example               # Environment configuration template
├── docker-compose.yml         # Kafka (KRaft), Kafka UI, Postgres, Grafana stack
├── pyproject.toml             # Python packaging and test configuration
├── requirements.txt           # Project dependencies
│
├── config/
│   └── pipeline.yaml          # Default pipeline hyperparameters
│
├── data/
│   ├── raw/                   # Raw benchmark datasets (AIOPS_KPI, SMD)
│   ├── processed/             # Cleaned evaluation artifacts
│   └── sample/                # Sample test streams
│
├── src/
│   └── agentic_streaming/
│       ├── config.py          # Type-safe Pydantic settings management
│       ├── schemas/           # Pydantic contracts (Metric, Anomaly, Agent, etc.)
│       ├── kafka/             # Replay producer and streaming consumer utilities
│       ├── streaming/         # Spark Structured Streaming pipeline
│       ├── aads/              # AADS streaming detector implementation
│       ├── detectors/         # StreamAD comparison detectors (xStream, HSTree, RRCF)
│       ├── agent/             # LangGraph state machine and decision logic
│       ├── tools/             # Analytical tools (statistics, drift, comparison)
│       ├── memory/            # Episodic vector store integration (ChromaDB)
│       ├── storage/           # Relational persistence models and queries
│       ├── metrics/           # Evaluation metrics (Detection, Streaming, Agent)
│       └── utils/             # Structured JSON logging and seed utilities
│
├── scripts/
│   └── check_env.py           # Environment and structure integrity validator
│
├── tests/
│   ├── conftest.py            # Global test fixtures
│   ├── unit/                  # Unit test suite
│   └── integration/           # End-to-end integration tests
│
├── experiments/
│   ├── configs/               # Reproducible experiment configurations
│   ├── baseline/              # Baseline evaluation outputs
│   ├── proposed/              # Proposed framework experiment runs
│   └── results/               # Comparative evaluation result tables and figures
│
├── dashboards/
│   └── grafana/               # Grafana dashboards and provisioning configs
│
├── notebooks/                 # Exploratory analysis and visual verification
│
└── docs/
    ├── architecture/          # Architectural specifications
    ├── experiments/           # Experimental protocol documentation
    └── report/                # Final academic report artifacts
```

---

## Getting Started

### Prerequisites
- Python 3.10+
- Docker & Docker Compose (for Kafka, PostgreSQL, Grafana)
- Java 8/11/17 (required for Apache Spark)

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/Tharunkunamalla/agentic-streaming-analytics.git
   cd agentic-streaming-analytics
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   # Windows:
   .venv\Scripts\activate
   # Linux / macOS:
   source .venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure environment variables:
   ```bash
   cp .env.example .env
   ```

### Verification & Testing
Run the environment verification script:
```bash
python scripts/check_env.py
```

Run the unit test suite:
```bash
pytest tests/unit -v
```

### Starting Infrastructure Services
Start the Kafka broker, Kafka UI, PostgreSQL, and Grafana containers:
```bash
docker-compose up -d
```
- **Kafka Broker:** `localhost:9092`
- **Kafka UI:** `http://localhost:8080`
- **PostgreSQL:** `localhost:5432`
- **Grafana:** `http://localhost:3000` (Default login: `admin` / `admin`)

---

## Roadmap

For the detailed phase-by-phase implementation plan and current milestone status, refer to [PHASES.md](PHASES.md).
