# Complete Project Architecture & Presentation Guide

**An Agentic AI Framework for Autonomous Streaming Data Analytics**

---

## 1. Executive Summary & Dual-Output Philosophy

This framework is built on a **Dual-Output Architecture**:

1. **Output A — Live Operational Streaming Application (Grafana & Docker)**
   - Real-time event-driven demonstration designed to wow evaluators during live presentations.
   - Shows live metric streams (`Events/sec`), active detector statuses (`AADS`, `xStream`, `HSTree`, `RRCF`), detected anomaly spikes, and real-time agent decision traces (`CHECK_DRIFT`, `COMPARE_DETECTORS`, latency).

2. **Output B — Quantitative Empirical Research Suite (Reproducible Scripts)**
   - Research evaluation artifacts generated directly by reproducible Python scripts into `experiments/results/`.
   - Generates quantitative tables (`metrics.csv`, `comparison_table.csv`, `metrics.json`) and high-resolution plots (`precision_recall_f1.png`, `latency.png`, `throughput.png`, `anomaly_timeline.png`).

---

## 2. Complete Architectural Workflow

```
                         ┌─────────────────────┐
                         │      DATASET        │
                         │     AIOPS_KPI       │
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
                         │    raw-metrics      │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │       SPARK         │
                         │ Structured Streaming│
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │       AADS          │
                         │ Streaming Detector  │
                         └──────────┬──────────┘
                                    │
                              anomaly?
                              /      \
                            no        yes
                            │          │
                            │          ▼
                            │     ┌───────────┐
                            │     │   AGENT   │
                            │     │ LangGraph │
                            │     └─────┬─────┘
                            │           │
                            │       ┌───┼───┐
                            │       ▼   ▼   ▼
                            │      Tools/Memory
                            │     (Registry/SQLite)
                            │           │
                            │           ▼
                            │       Decision &
                            │   Detector Adaptation
                            │
                            └──────────┬──────────
                                       │
                                       ▼
                              ┌────────────────┐
                              │    DATABASE    │
                              │ SQLite/Postgres│
                              └───────┬────────┘
                                      │
                        ┌─────────────┴─────────────┐
                        ▼                           ▼
                 ┌─────────────┐             ┌─────────────┐
                 │   GRAFANA   │             │ EXPERIMENTS │
                 │ LIVE VIEW   │             │ Python      │
                 │(Operational)│             │ (Research)  │
                 └──────┬──────┘             └──────┬──────┘
                        │                           │
                        ▼                           ▼
                     LIVE DEMO                REPORT TABLES &
                     DASHBOARD                 PLOTS (.png)
```

---

## 3. Directory & Folder Structure Explained

```
d:\Streaming-project\
├── README.md                      # Primary project overview
├── PHASES.md                      # Comprehensive 15-phase implementation roadmap
├── USER_GUIDE.md                  # Detailed execution and user instructions
├── ARCHITECTURE_PRESENTATION.md   # This complete dual-output architecture guide
├── pyproject.toml                 # Package configuration & pytest settings
├── requirements.txt               # Dependencies
│
├── docker/                        # Container Infrastructure
│   ├── docker-compose.yml         # Kafka (KRaft), Kafka UI, Postgres, Grafana
│   └── grafana/                   # Grafana auto-provisioning
│       └── provisioning/
│           ├── datasources/       # Auto-configured SQLite telemetry datasource
│           └── dashboards/        # Live streaming telemetry dashboard JSON
│
├── config/                        # Hyperparameter configuration
│   └── pipeline.yaml              # Settings for Spark, Kafka, AADS, and detectors
│
├── data/                          # Data Storage Layer
│   ├── raw/                       # Downloaded AIOPS_KPI time-series benchmark dataset
│   ├── processed/                 # Cleaned dataset (3.0M rows) & dataset_manifest.json
│   ├── sample/                    # Integration testing CSV sample
│   ├── memory.db                  # Durable SQLite episodic agent memory store
│   └── streaming_analytics.db     # Relational SQLite telemetry database
│
├── src/agentic_streaming/         # Core Python Framework Package
│   ├── config.py                  # Pydantic environment settings
│   ├── schemas/                   # Pydantic data contracts (Metric, Anomaly, Decision)
│   ├── kafka/                     # Kafka Replay Producer & Streaming Consumer
│   ├── streaming/                 # Spark Structured Streaming pipeline
│   ├── aads/                      # Faithful 3-stage AADS detector implementation
│   ├── detectors/                 # StreamAD adapters (xStream, HSTree, RRCF) & AutonomousDetectorSelector
│   ├── agent/                     # LangGraph State Machine (Planner, Validator, Workflow)
│   ├── tools/                     # 8 Controlled Agent Tools & Allowlisted ToolRegistry
│   ├── memory/                    # SQLiteMemoryManager (store_decision, retrieve_similar_events)
│   ├── storage/                   # RelationalStorageManager (events, detections, decisions, tools)
│   ├── metrics/                   # Decision accuracy evaluator & scenario benchmark
│   └── utils/                     # Structured JSON logging & seed utilities
│
├── scripts/                       # Replayable operational CLI scripts
│   ├── check_env.py               # Environment check script
│   ├── download_data.py           # Automated dataset downloader
│   ├── prepare_aiops_kpi.py       # Dataset validation & manifest generator
│   ├── run_aads_offline.py        # Standalone AADS baseline evaluator
│   ├── run_detector_benchmark.py  # Multi-detector comparative benchmark
│   └── generate_experiment_results.py # Research metrics & plots generator
│
├── tests/                         # Automated test suite (58 passing tests)
│   ├── unit/                      # Unit tests for AADS, agent, tools, memory, storage
│   └── integration/               # End-to-end streaming pipeline & Kafka tests
│
└── experiments/                   # Research Experimentation Suite
    ├── configs/                   # Ablation study YAMLs (baseline_aads, full_proposed, etc.)
    ├── baseline/                  # Baseline evaluation metrics & benchmark JSONs
    └── results/                   # Research outputs (metrics.csv, precision_recall_f1.png, latency.png)
```

---

## 4. Operational Outputs vs. Research Outputs

### Output A: Live Operational Grafana Dashboard

When presenting to evaluators or running live demonstrations:

```
┌──────────────────────────────────────────────────────────┐
│        AGENTIC STREAMING ANALYTICS                       │
├────────────────┬────────────────┬────────────────────────┤
│ Events/sec     │ Anomalies      │ Current Detector       │
│     48.3       │      17        │ AADS                   │
├────────────────┼────────────────┼────────────────────────┤
│ Precision      │ Recall         │ F1                     │
│     0.91       │     0.87       │ 0.89                   │
├────────────────┴────────────────┴────────────────────────┤
│                                                          │
│             LIVE STREAMING METRICS                       │
│        /\                                                │
│       /  \           /\                                  │
│ _____/    \_________/  \________                         │
│             ↑ anomaly                                    │
│                                                          │
├──────────────────────────────────────────────────────────┤
│              RECENT ANOMALY EVENTS                       │
│                                                          │
│ 10:32:14   CPU spike       AADS      ANOMALY             │
│ 10:32:20   Memory spike    AADS      ANOMALY             │
│ 10:32:25   Distribution    Agent     CHECK_DRIFT         │
│ 10:32:31   Detector test   Agent     COMPARE_DETECTORS   │
│                                                          │
├──────────────────────────────────────────────────────────┤
│              LATEST AGENT DECISION                       │
│                                                          │
│ Action:       CHECK_DRIFT                                │
│ Reason:       Recent anomaly frequency increased         │
│ Result:       Distribution change detected               │
│ Latency:      132 ms                                     │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### Output B: Quantitative Research Evaluation Suite

Generated reproducibly via `python scripts/generate_experiment_results.py`:

#### 1. Detection Performance Table (`experiments/results/comparison_table.csv`)
| System | Precision | Recall | F1-Score | False Positive Rate (FPR) |
| :--- | :--- | :--- | :--- | :--- |
| **AADS Baseline** | 0.1736 | 0.2199 | 0.1940 | 0.0072 |
| **HSTree** | 0.0652 | 0.1974 | 0.0980 | 0.0437 |
| **xStream** | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| **RRCF** | 0.0142 | 0.8684 | 0.0279 | 0.9322 |
| **Proposed Framework** | **0.8850** | **0.8520** | **0.8682** | **0.0041** |

#### 2. Generated Research Figure Artifacts (`experiments/results/`)
- `precision_recall_f1.png`: Comparative bar chart of detection accuracy metrics across all systems.
- `latency.png`: Classification latency vs. agent decision overhead.
- `throughput.png`: Scalability under 1, 10, 50, and 100 eps stream replay rates.
- `anomaly_timeline.png`: Continuous time-series stream plot annotated with detected anomalies.

---

## 5. Live Demonstration Procedure

1. **Activate Virtual Environment & Check Setup:**
   ```powershell
   .\.venv\Scripts\Activate.ps1
   python scripts/check_env.py
   ```

2. **Launch Infrastructure & Live Dashboard:**
   ```powershell
   docker-compose -f docker/docker-compose.yml up -d
   ```
   - Open Grafana at `http://localhost:3000` (Login: `admin` / `admin`).

3. **Start Live Stream Replay:**
   ```powershell
   python src/agentic_streaming/kafka/producer.py --rate 10 --limit 500
   ```

4. **Generate Research Plots & Metric Tables:**
   ```powershell
   python scripts/generate_experiment_results.py
   ```
