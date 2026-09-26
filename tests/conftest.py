"""Pytest global configuration and fixtures."""

import os
import sys
from pathlib import Path
import pytest

# Add src to python path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# Windows compatibility patch for kafka-python / selectors
if sys.platform == "win32":
    import selectors

    if hasattr(selectors, "SelectSelector") and not getattr(selectors.SelectSelector, "_is_patched_for_windows", False):
        def _safe_select_unregister(self, fileobj):
            try:
                key = super(selectors.SelectSelector, self).unregister(fileobj)
            except (KeyError, ValueError):
                return None
            if key is not None:
                self._readers.discard(key.fd)
                self._writers.discard(key.fd)
            return key

        selectors.SelectSelector.unregister = _safe_select_unregister
        selectors.SelectSelector._is_patched_for_windows = True


@pytest.fixture(autouse=True)
def reset_settings_env(monkeypatch, tmp_path):
    """Ensure clean environment variables and test directories for each test."""
    test_log_dir = tmp_path / "logs"
    monkeypatch.setenv("LOG_DIR", str(test_log_dir))
    monkeypatch.setenv("APP_ENV", "testing")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    yield
