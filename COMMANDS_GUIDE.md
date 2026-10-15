# 🚀 Agentic Streaming Analytics — Operational Run Guide

A quick reference guide for running all commands, continuous data generators, anomaly injectors, virtual environments, and monitoring interfaces.

---

## 1. 🐍 Virtual Environment (.venv) Usage

### When to use `.venv`:
> [!IMPORTANT]
> **Always activate `.venv` before running any Python script or command in this project.** This ensures that all required dependencies (`kafka-python`, `pandas`, `pydantic`, `sqlite3`, etc.) are properly isolated and accessible.

### Activation Commands (Windows PowerShell):
```powershell
# Navigate to the project directory
cd d:\Streaming-project

# Activate the virtual environment
.venv\Scripts\Activate.ps1
```
*(You will see `(.venv)` appear at the beginning of your command prompt line).*

### If running in Command Prompt (CMD):
```cmd
.venv\Scripts\activate.bat
```

---

## 2. 🐳 Starting the Project Infrastructure (Docker)

Start Kafka, PostgreSQL, Grafana, and Kafka UI in background containers:

```powershell
# Start all containers in detached mode
docker-compose up -d

# Verify all containers are running and healthy
docker ps
```

### Access URLs:
- **Grafana Dashboard**: [http://localhost:3000](http://localhost:3000) (User: `admin` | Password: `admin`)
- **Kafka UI**: [http://localhost:8080](http://localhost:8080)
- **PostgreSQL**: `localhost:5432` (`streaming_analytics` / `postgrespassword`)

---

## 3. 🌊 Running Real-Time Continuous Streaming

To see the Grafana graph **move and scroll continuously** with real-time incoming telemetry:

```powershell
# Run continuous live stream (1 event/sec, automatic anomaly chance)
python scripts/run_continuous_stream.py
```

### Customizing Continuous Stream Options:
```powershell
# Stream faster (e.g., 5 events per second)
python scripts/run_continuous_stream.py --rate 5.0

# Increase or decrease anomaly probability (e.g., 30% chance of spikes)
python scripts/run_continuous_stream.py --rate 2.0 --anomaly-prob 0.30
```
> Press `Ctrl + C` in the terminal at any time to pause or stop the stream.

---

## 4. ⚡ Injecting Specific Unusual Behavior (Anomalies)

To test an immediate spike / critical incident on demand:

```powershell
# Inject 10 CPU spikes exceeding normal thresholds (>110%)
python scripts/inject_unusual_behavior.py --metric CPU --value 110.5 --spikes 10

# Inject Memory anomaly spikes
python scripts/inject_unusual_behavior.py --metric MEMORY --value 98.0 --spikes 5

# Inject with a specific detector (e.g., AADS)
python scripts/inject_unusual_behavior.py --detector AADS --value 120.0 --spikes 15
```

---

## 5. 🤖 Terminal Live AI Detective Viewer

To watch the AI Agent explain its automated triage decisions and actions in the terminal:

```powershell
python scripts/live_ai_dashboard.py
```

---

## 6. 📊 Benchmark & Evaluation Scripts

To benchmark and evaluate detection accuracy across algorithms:

```powershell
# Run baseline comparison (AADS vs. StreamAD algorithms)
python scripts/evaluate_aads_baseline.py

# Run streaming detector comparison benchmark
python scripts/run_streamad_comparison.py

# Verify Kafka stream ingestion
python scripts/verify_kafka_stream.py
```

---

## 7. 🛑 Stopping the Project

When you are done with your session:

```powershell
# Stop all Docker services safely
docker-compose down

# Deactivate the virtual environment
deactivate
```

---

## 📋 Quick Command Cheat Sheet

| Task | Command |
| :--- | :--- |
| **Activate `.venv`** | `.venv\Scripts\Activate.ps1` |
| **Start Infrastructure** | `docker-compose up -d` |
| **Continuous Live Stream** | `python scripts/run_continuous_stream.py` |
| **Inject Anomaly Spikes** | `python scripts/inject_unusual_behavior.py --value 110.5 --spikes 10` |
| **Live AI Terminal Viewer** | `python scripts/live_ai_dashboard.py` |
| **Open Grafana UI** | [http://localhost:3000](http://localhost:3000) |
| **Open Kafka UI** | [http://localhost:8080](http://localhost:8080) |
| **Stop Infrastructure** | `docker-compose down` |
