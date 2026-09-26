"""Unit tests for configuration loading and validation."""

import pytest
from pydantic import ValidationError
from src.config.settings import Settings, get_settings


def test_default_settings():
    """Verify default settings load cleanly with valid expected values."""
    settings = Settings()
    assert settings.app_name == "agentic-streaming-analytics"
    assert settings.kafka_bootstrap_servers == "localhost:9092"
    assert settings.kafka_raw_metrics_topic == "raw-metrics"
    assert settings.kafka_anomaly_events_topic == "anomaly-events"
    assert settings.kafka_agent_decisions_topic == "agent-decisions"
    assert settings.default_detector == "AADS"
    assert settings.aads_window_size == 100
    assert settings.aads_anomaly_threshold == 3.0
    assert settings.llm_temperature == 0.0
    assert settings.random_seed == 42


def test_env_override(monkeypatch):
    """Verify environment variables correctly override configuration fields."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "cluster.kafka:9094")
    monkeypatch.setenv("AADS_ANOMALY_THRESHOLD", "4.5")
    monkeypatch.setenv("APP_ENV", "production")

    settings = Settings()
    assert settings.kafka_bootstrap_servers == "cluster.kafka:9094"
    assert settings.aads_anomaly_threshold == 4.5
    assert settings.app_env == "production"


def test_invalid_settings_validation():
    """Verify pydantic validation errors for out-of-range configurations."""
    with pytest.raises(ValidationError):
        # replay_speed_multiplier must be > 0
        Settings(replay_speed_multiplier=-1.0)

    with pytest.raises(ValidationError):
        # aads_decay_factor must be <= 1.0
        Settings(aads_decay_factor=1.5)


def test_singleton_getter():
    """Verify get_settings returns a valid Settings instance."""
    settings = get_settings()
    assert isinstance(settings, Settings)
