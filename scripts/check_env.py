"""Environment & Repository Integrity Check Script for Phase 1.

Validates:
1. Python version compatibility (>= 3.10).
2. Key required packages (Pydantic, NumPy, SciPy, PySpark, LangGraph, etc.).
3. Directory structure layout.
4. Settings loading and validation.
5. Structured logger initialization.
"""

import sys
from pathlib import Path

# Ensure src is on Python path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

REQUIRED_PACKAGES = [
    "pydantic",
    "pydantic_settings",
    "pythonjsonlogger",
    "numpy",
    "pandas",
    "scipy",
    "sklearn",
    "pytest",
]

OPTIONAL_PACKAGES = [
    "pyspark",
    "kafka",
    "langgraph",
    "langchain",
    "sqlalchemy",
]

REQUIRED_DIRS = [
    REPO_ROOT / "src" / "config",
    REPO_ROOT / "src" / "schemas",
    REPO_ROOT / "src" / "utils",
    REPO_ROOT / "src" / "producer",
    REPO_ROOT / "src" / "streaming",
    REPO_ROOT / "src" / "detectors",
    REPO_ROOT / "src" / "agent",
    REPO_ROOT / "src" / "storage",
    REPO_ROOT / "src" / "evaluation",
    REPO_ROOT / "data" / "raw",
    REPO_ROOT / "data" / "processed",
    REPO_ROOT / "docker",
    REPO_ROOT / "tests" / "unit",
    REPO_ROOT / "tests" / "integration",
    REPO_ROOT / "logs",
]


def check_python_version() -> bool:
    print(f"[1/5] Python Version: {sys.version.split()[0]} ... ", end="")
    if sys.version_info >= (3, 10):
        print("OK")
        return True
    print("FAILED (Requires Python >= 3.10)")
    return False


def check_directories() -> bool:
    print("[2/5] Verifying Directory Layout ... ", end="")
    missing = [str(d.relative_to(REPO_ROOT)) for d in REQUIRED_DIRS if not d.exists()]
    if not missing:
        print("OK (All required directories present)")
        return True
    print(f"FAILED (Missing directories: {missing})")
    return False


def check_dependencies() -> bool:
    print("[3/5] Checking Core Dependencies ...")
    all_ok = True
    for pkg in REQUIRED_PACKAGES:
        try:
            __import__(pkg)
            print(f"  - {pkg}: OK")
        except ImportError as e:
            print(f"  - {pkg}: MISSING ({e})")
            all_ok = False

    print("  Checking Streaming/Agent Ecosystem Dependencies ...")
    for pkg in OPTIONAL_PACKAGES:
        try:
            __import__(pkg)
            print(f"  - {pkg}: Available")
        except ImportError:
            print(f"  - {pkg}: Not installed (will be required for subsequent phases)")
    return all_ok


def check_settings_and_logging() -> bool:
    print("[4/5] Testing Configuration & Structured Logging ... ", end="")
    try:
        from src.config.settings import Settings, get_settings
        from src.utils.logger import setup_logger
        from src.utils.seed import set_seed

        settings = get_settings()
        assert isinstance(settings, Settings)
        assert settings.app_name == "agentic-streaming-analytics"

        set_seed(settings.random_seed)
        logger = setup_logger("env_check", log_level="INFO", log_format="json")
        logger.info("Environment check executed successfully", extra={"test_key": "val"})
        print("OK")
        return True
    except Exception as e:
        print(f"FAILED ({e})")
        return False


def check_schemas() -> bool:
    print("[5/5] Testing Pydantic Schemas ... ", end="")
    try:
        from datetime import datetime, timezone
        from src.schemas.metric import MetricRecord
        from src.schemas.anomaly import AnomalyEvent
        from src.schemas.agent import AgentDecision, AgentActionType
        from src.schemas.detector import DetectorType

        now = datetime.now(timezone.utc)
        m = MetricRecord(timestamp=now, metric_id="cpu_util", value=92.5)
        a = AnomalyEvent(
            timestamp=now,
            metric_id="cpu_util",
            value=92.5,
            anomaly_score=4.2,
            threshold=3.0,
            window_mean=50.0,
            window_std=10.0,
            recent_window=[48.0, 49.0, 52.0, 92.5],
        )
        d = AgentDecision(
            event_id=a.event_id,
            metric_id=a.metric_id,
            action=AgentActionType.CHECK_DRIFT,
            confidence=0.95,
            reasoning="Sudden 4-sigma spike detected after stationary baseline.",
            selected_detector=DetectorType.AADS,
        )
        assert m.value == 92.5
        assert a.anomaly_score == 4.2
        assert d.action == AgentActionType.CHECK_DRIFT
        print("OK")
        return True
    except Exception as e:
        print(f"FAILED ({e})")
        return False


def main() -> int:
    print("=" * 60)
    print("Phase 1 Environment & Architecture Verification")
    print("=" * 60)
    results = [
        check_python_version(),
        check_directories(),
        check_dependencies(),
        check_settings_and_logging(),
        check_schemas(),
    ]
    print("=" * 60)
    if all(results):
        print("Phase 1 Verification: SUCCESS (Environment is ready)")
        return 0
    else:
        print("Phase 1 Verification: FAILED (Resolve issues above)")
        return 1


if __name__ == "__main__":
    sys.exit(main())
