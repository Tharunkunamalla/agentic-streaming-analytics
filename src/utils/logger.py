"""Structured logging setup for the agentic streaming framework.

Provides JSON-formatted file and console logging with context-rich metadata.
"""

import logging
import os
import sys
from pathlib import Path
from typing import Any

from pythonjsonlogger import jsonlogger

from src.config.settings import get_settings


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """Custom JSON formatter adding standard timestamp and level fields."""

    def add_fields(
        self,
        log_record: dict[str, Any],
        record: logging.LogRecord,
        message_dict: dict[str, Any],
    ) -> None:
        super().add_fields(log_record, record, message_dict)
        if not log_record.get("timestamp"):
            log_record["timestamp"] = self.formatTime(record, self.datefmt)
        if log_record.get("level"):
            log_record["level"] = log_record["level"].upper()
        else:
            log_record["level"] = record.levelname


def setup_logger(
    name: str = "agentic_streaming",
    log_level: str | None = None,
    log_format: str | None = None,
    log_dir: Path | str | None = None,
) -> logging.Logger:
    """Configure and return a structured logger instance.

    Parameters
    ----------
    name : str
        The logger hierarchy name.
    log_level : str, optional
        Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL). If None, uses Settings.
    log_format : str, optional
        'json' or 'text'. If None, uses Settings.
    log_dir : Path or str, optional
        Target directory for log files. If None, uses Settings.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """
    settings = get_settings()
    level_str = log_level or settings.log_level
    format_str = log_format or settings.log_format
    target_dir = Path(log_dir or settings.log_dir)

    target_dir.mkdir(parents=True, exist_ok=True)
    numeric_level = getattr(logging, level_str.upper(), logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(numeric_level)

    # Avoid duplicate handlers on re-initialization
    if logger.handlers:
        return logger

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)

    if format_str == "json":
        json_formatter = CustomJsonFormatter(
            "%(timestamp)s %(level)s %(name)s %(message)s"
        )
        console_handler.setFormatter(json_formatter)
    else:
        text_formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(text_formatter)

    logger.addHandler(console_handler)

    # File Handler (Always structured JSON for audit trail)
    log_file = target_dir / f"{name}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(numeric_level)
    file_formatter = CustomJsonFormatter(
        "%(timestamp)s %(level)s %(name)s %(message)s %(pathname)s %(lineno)d"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger


def get_logger(name: str = "agentic_streaming") -> logging.Logger:
    """Get or initialize a logger instance."""
    return setup_logger(name)
