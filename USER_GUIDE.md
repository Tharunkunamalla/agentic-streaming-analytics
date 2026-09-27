# Complete Project Documentation & User Guide

**An Agentic AI Framework for Autonomous Streaming Data Analytics**

---

## 1. What is this Project?

High-velocity cloud telemetry metrics (CPU utilization, network traffic, memory saturation) require sub-millisecond anomaly detection. Traditional algorithms (like AADS, xStream, HSTree, RRCF) are extremely fast at detecting statistical outliers, but they produce high false positive rates, cannot explain root causes, and cannot dynamically adapt when concept drift occurs.

This project implements a **two-stage hybrid architecture**:

1. **Stage 1: High-Speed Streaming Detection Layer**
   - Built on **Apache Kafka** & **Apache Spark Structured Streaming**.
   - Processes streaming metrics in real time (thousands of events per second, sub-millisecond classification latency) using **AADS** (*Autonomous Anomaly Detection for Streaming Data*).
   - Only flags candidate anomalies and pushes structured context events to the `anomaly-events` Kafka topic. Nominal traffic is processed with zero LLM overhead.

2. **Stage 2: Agentic AI Orchestration Layer**
   - Built using a **LangGraph state machine**.
   - Receives compact event context (`current_event`, `recent_window`, `anomaly`, `detector`).
   - Executes controlled analytical tools via an allowlisted **`ToolRegistry`** (`calculate_statistics`, `check_drift`, `compare_detectors`, `retrieve_similar_events`, `store_decision`).
   - Retrieves historical incident resolutions from **SQLite Memory**.
   - Evaluates empirical test results using a **Deterministic Detector Adaptation Engine** to dynamically switch streaming detectors when concept drift occurs.
   - Logs complete operational telemetry to a relational database and visualizes real-time metrics on a **Grafana Dashboard**.

---

## 2. Directory & Folder Structure Explained

```
d:\Streaming-project\
├── README.md                      # Primary project overview
├── PHASES.md                      # Detailed phase progress tracking (Phases 1-14 Completed)
├── USER_GUIDE.md                  # This complete user, execution, and architecture guide
├── pyproject.toml                 # Package definition & pytest config
├── requirements.txt               # Dependencies
│
├── docker/                        # Infrastructure container definitions
│   ├── docker-compose.yml         # Kafka (KRaft), Kafka UI, Postgres, Grafana stack
│   └── grafana/                   # Grafana provisioning
│       └── provisioning/
│           ├── datasources/       # Auto-configured SQLite datasource
│           └── dashboards/        # Real-time streaming telemetry dashboard JSON
│
├── config/                        # System settings & pipeline YAMLs
│   └── pipeline.yaml              # Hyperparameters for AADS, Spark, and Kafka
│
├── data/                          # Data storage
│   ├── raw/                       # Downloaded AIOPS_KPI time-series benchmark dataset
│   ├── processed/                 # Cleaned dataset (3.0M rows) & dataset_manifest.json
│   ├── sample/                    # Integration testing CSV sample
│   ├── memory.db                  # Durable SQLite episodic agent memory store
│   └── streaming_analytics.db     # Relational SQLite telemetry database
│
├── src/agentic_streaming/         # Core Python Framework Package
│   ├── config.py                  # Pydantic environment configuration
│   ├── schemas/                   # Strongly typed contracts (Metric, Anomaly, Decision)
│   ├── kafka/                     # Kafka Replay Producer & Consumer
│   ├── streaming/                 # Spark Structured Streaming pipeline
│   ├── aads/                      # Faithful 3-stage AADS detector implementation
│   ├── detectors/                 # StreamAD adapters (xStream, HSTree, RRCF) & AutonomousDetectorSelector
│   ├── agent/                     # LangGraph State Machine (Planner, Validator, Workflow)
│   ├── tools/                     # 8 Controlled Agent Tools & Allowlisted ToolRegistry
│   ├── memory/                    # SQLiteMemoryManager (store_decision, retrieve_similar_events)
│   ├── storage/                   # RelationalStorageManager (events, detections, decisions, tools)
│   ├── metrics/                   # Decision accuracy evaluator & benchmark tools
│   └── utils/                     # Structured JSON logging & random seed control
│
├── agent/                         # Root forwarding module for LangGraph agent
├── memory/                        # Root forwarding module for memory store
├── tools/                         # Root forwarding module for tool registry
├── storage/                       # Root forwarding module for relational database
│
├── scripts/                       # Replayable operational CLI scripts
│   ├── download_data.py           # Automated dataset downloader
│   ├── prepare_aiops_kpi.py       # Dataset validation & manifest generator
│   ├── run_aads_offline.py        # Standalone AADS baseline evaluator
│   └── run_detector_benchmark.py  # Multi-detector comparative benchmark
│
├── tests/                         # Full automated test suite (58 passing tests)
│   ├── unit/                      # Unit tests for AADS, agent, tools, memory, storage
│   └── integration/               # End-to-end streaming pipeline & Kafka tests
│
└── experiments/                   # Experimentation & Ablation Study Configurations
    ├── configs/                   # Experiment YAMLs (baseline_aads, full_proposed, etc.)
    ├── baseline/                  # Baseline evaluation metrics & benchmark JSONs
    └── results/                   # Evaluation result plots (precision_recall_f1.png, latency.png)
```

---

## 3. How to Run & Test the Project

### Step 1: Environment Setup
Ensure Python 3.10+ and Docker are installed.
```powershell
# 1. Create virtual environment (if not already created)
python -m venv .venv

# 2. Activate virtual environment in PowerShell
.\.venv\Scripts\Activate.ps1
# Or in CMD:
# .venv\Scripts\activate.bat

# 3. Verify environment integrity
python scripts/check_env.py
```

<!-- .\.venv\Scripts\Activate.ps1
python scripts/check_env.py -->


### Step 2: Run the Unit & Integration Test Suite
Execute the comprehensive test suite verifying detector algorithms, agent state machine, tool registry, and memory persistence:
```bash
# Run all unit tests
pytest tests/unit -v

# Run Phase 9 & Phase 10 tool/memory tests
pytest tests/unit/test_tools.py tests/unit/test_memory.py -v

# Run Phase 11 & Phase 12 adaptation/storage tests
pytest tests/unit/test_phase11_phase12.py -v

# Run full test suite (58 tests)
pytest -v
```

### Step 3: Start Docker Infrastructure
Launch the Apache Kafka KRaft broker, Kafka UI, PostgreSQL, and Grafana containers:
```bash
docker-compose -f docker/docker-compose.yml up -d
```
Access endpoints:
- **Kafka UI:** `http://localhost:8080`
- **Grafana Dashboard:** `http://localhost:3000` (Login: `admin` / `admin`)

### Step 4: Run Real-Time Kafka Replay Producer
Replay real AIOPS_KPI dataset metric events into Kafka topic `raw-metrics`:
```bash
# Replay 500 events at 10 events/sec speed
python src/agentic_streaming/kafka/producer.py --rate 10 --limit 500
```

### Step 5: Run Standalone Baseline & Detector Benchmark
```bash
# Evaluate AADS standalone baseline on 50,000 cloud KPI metrics
python scripts/run_aads_offline.py

# Benchmark comparative detectors (HSTree, xStream, RRCF)
python scripts/run_detector_benchmark.py
```

---

## 4. How to Explain This Project in an Interview or Presentation

When presenting or explaining this project, emphasize the following **4 key narrative pillars**:

### Pillar 1: The Problem We Solve
> "High-speed streaming metrics in cloud platforms produce millions of metric points per second. Traditional streaming detectors can classify outliers rapidly but suffer from high false positive rates and cannot adapt when concept drift occurs. On the other hand, calling an LLM on every raw metric point is impossible due to latency overhead and API cost."

### Pillar 2: Our Novel Two-Stage Architecture
> "We built a two-stage hierarchical architecture. Stage 1 uses Apache Spark Structured Streaming and the AADS algorithm to process thousands of events per second with sub-millisecond latency. Stage 1 filters out 99% of normal stream traffic. Only candidate anomalies are pushed to Kafka as compact context payloads (`current_event`, `recent_window`, `anomaly`, `detector`). Stage 2 invokes a LangGraph AI Agent only when anomalies occur."

### Pillar 3: Controlled Analytical Tools & Deterministic Adaptation
> "The agent does NOT execute arbitrary shell code or make ungrounded decisions. Instead, it interacts with an allowlisted `ToolRegistry` to run precise statistical tools (`calculate_statistics`, `check_drift`, `compare_detectors`). If drift is detected (measured via Kolmogorov-Smirnov test and Population Stability Index > 0.20), a **Deterministic Adaptation Engine** automatically switches the active streaming detector to the best-performing algorithm (e.g., switching to xStream or HSTree) for the next evaluation window."

### Pillar 4: Empirical Rigor & Full O---

## 5. The Three Output Layers & Presentation Strategy

Our project produces **three distinct output layers**, each serving a specific engineering and academic evaluation purpose:

### Layer 1: Terminal Log Stream (Engineering & Debugging)
Provides real-time CLI logs during execution:
```
[Kafka] Connected to bootstrap server localhost:9092
[Producer] Replaying event 1245 (value=92.5, ground_truth=1)
[Spark] Processed 1245 micro-batch events
[AADS] Candidate anomaly detected (score=0.91) -> Published to anomaly-events
[Agent] Action selected: CHECK_DRIFT
[Tool] check_drift executed in 42 ms (PSI=0.28, KS-pvalue=0.008)
[Agent] Decision: POSSIBLE_CONCEPT_DRIFT -> Active detector adapted to xStream
```

### Layer 2: Research Evaluation & Report Artifacts (Scientific Benchmark)
Provides reproducible quantitative evaluation results saved in `experiments/results/`:
- `metrics.csv` & `metrics.json`
- `precision_recall_f1.png`
- `latency.png`
- `throughput.png`
- `anomaly_timeline.png`
- `comparison_table.csv`

#### Result Metrics Evaluated:
- **Detection Performance:** Precision, Recall, F1-Score, False Positive Rate (FPR).
- **Streaming Telemetry:** Throughput (events/sec), Classification Latency (ms), Memory Usage (MB).
- **Agent Telemetry:** Decision Accuracy (%), Decision Success Rate (%), Avg Decision Latency (ms), Unnecessary Tool Calls.
- **Adaptation Telemetry:** Concept Drift Detection Accuracy, Detector Switch Frequency, Recovery Time.

### Layer 3: Real-Time Grafana Application Dashboard (Live Demonstration UI)
The primary live presentation application. When the professor views the demonstration:
1. **Live Stream Active:** `Events/sec: 50` updates continuously.
2. **Anomaly Arrives:** Counter increments `Anomalies: 1` and marks anomaly spike on live stream graph.
3. **Agent Triage Triggers:** `Latest Agent Decision` panel updates:
   - *Action:* `CHECK_DRIFT`
   - *Result:* `Distribution change detected (PSI=0.28)`
4. **Autonomous Adaptation:** `Current Detector` status updates from `AADS` $\rightarrow$ `xStream`.

---

## 6. System Architecture Diagram

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
```�──────────┐
                                  │ LangGraph Agent    │
                                  └──────────┬─────────┘
                                             │
                       ┌─────────────────────┼─────────────────────┐
                       ▼                     ▼                     ▼
                Check Drift          Compare Detectors     Episodic Memory
                 (KS / PSI)            (StreamAD)             (SQLite)
                       │                     │                     │
                       └─────────────────────┼─────────────────────┘
                                             ▼
                                Deterministic Policy Engine
                                 (Selects Best Detector)
                                             │
                                             ▼
                               Relational DB & Grafana
```
