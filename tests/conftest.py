"""Pytest global configuration and fixtures."""

import os
import sys
from pathlib import Path
import pytest

# Add src to python path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(autouse=True)
def reset_settings_env(monkeypatch, tmp_path):
    """Ensure clean environment variables and test directories for each test."""
    test_log_dir = tmp_path / "logs"
    monkeypatch.setenv("LOG_DIR", str(test_log_dir))
    monkeypatch.setenv("APP_ENV", "testing")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    yield
