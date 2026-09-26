"""Unit tests for structured logging and seed utilities."""

import json
import logging
from pathlib import Path
import numpy as np

from src.utils.logger import setup_logger, get_logger
from src.utils.seed import set_seed


def test_structured_logger_json(tmp_path):
    """Verify structured logger writes valid JSON log entries to file."""
    log_dir = tmp_path / "logs"
    logger = setup_logger("test_json_logger", log_level="DEBUG", log_format="json", log_dir=log_dir)

    logger.info("Test message 1", extra={"metric_id": "kpi_01", "anomaly_score": 3.8})

    log_file = log_dir / "test_json_logger.log"
    assert log_file.exists()

    with open(log_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        assert len(lines) >= 1
        record = json.loads(lines[-1])
        assert record["message"] == "Test message 1"
        assert record["level"] == "INFO"
        assert record["name"] == "test_json_logger"


def test_structured_logger_text(tmp_path):
    """Verify text formatted logger operates without error."""
    log_dir = tmp_path / "logs_text"
    logger = setup_logger("test_text_logger", log_level="INFO", log_format="text", log_dir=log_dir)
    logger.info("Standard text log entry")
    assert (log_dir / "test_text_logger.log").exists()


def test_set_seed_reproducibility():
    """Verify set_seed produces deterministic pseudo-random sequences."""
    set_seed(1234)
    sample_a = np.random.randn(5)

    set_seed(1234)
    sample_b = np.random.randn(5)

    assert np.allclose(sample_a, sample_b)
