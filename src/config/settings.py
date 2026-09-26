"""Configuration management module using Pydantic Settings.

Loads configurations from environment variables and .env files with strong typing
and schema validation.
"""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Global Application Settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Application & Environment ---
    app_env: Literal["development", "testing", "production"] = Field(
        default="development", description="Execution environment mode"
    )
    app_name: str = Field(
        default="agentic-streaming-analytics", description="Application identifier"
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO", description="Logging verbosity level"
    )
    log_format: Literal["json", "text"] = Field(
        default="json", description="Structured JSON or standard console text format"
    )
    log_dir: Path = Field(
        default=Path("logs"), description="Directory where log files are written"
    )
    random_seed: int = Field(
        default=42, description="Global random seed for deterministic reproduction"
    )

    # --- Kafka Streaming Broker ---
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092", description="Kafka bootstrap broker list"
    )
    kafka_raw_metrics_topic: str = Field(
        default="raw-metrics", description="Topic for raw replayed time-series stream"
    )
    kafka_anomaly_events_topic: str = Field(
        default="anomaly-events", description="Topic for detected anomaly candidate events"
    )
    kafka_agent_decisions_topic: str = Field(
        default="agent-decisions", description="Topic for published autonomous agent decisions"
    )
    kafka_consumer_group_streaming: str = Field(
        default="spark-streaming-group", description="Consumer group ID for Spark streaming"
    )
    kafka_consumer_group_agent: str = Field(
        default="langgraph-agent-group", description="Consumer group ID for Agent event consumer"
    )
    kafka_auto_offset_reset: Literal["earliest", "latest"] = Field(
        default="latest", description="Offset reset policy for Kafka consumers"
    )

    # --- Spark Structured Streaming ---
    spark_app_name: str = Field(
        default="AgenticStreamingAnomalyDetection", description="Spark application name"
    )
    spark_master: str = Field(
        default="local[*]", description="Spark master cluster or local mode"
    )
    spark_checkpoint_dir: Path = Field(
        default=Path("./data/checkpoints/spark"), description="Directory for Spark streaming state checkpoints"
    )
    spark_trigger_processing_time: str = Field(
        default="1 second", description="Spark Structured Streaming micro-batch trigger interval"
    )

    # --- Dataset & Replay Producer ---
    dataset_name: str = Field(
        default="AIOPS_KPI", description="Active dataset name (e.g. AIOPS_KPI, SMD)"
    )
    dataset_path: Path = Field(
        default=Path("./data/raw/aiops_kpi.csv"), description="Path to raw dataset CSV"
    )
    replay_speed_multiplier: float = Field(
        default=1.0, gt=0.0, description="Speedup multiplier for stream replay (1.0 = real-time)"
    )
    replay_batch_size: int = Field(
        default=1, ge=1, description="Number of metric points per emitted Kafka payload"
    )

    # --- Baseline Streaming Detector (AADS KBS 2024) ---
    default_detector: str = Field(
        default="AADS", description="Primary streaming baseline anomaly detector"
    )
    aads_window_size: int = Field(
        default=100, ge=10, description="Sliding window size for streaming baseline statistics"
    )
    aads_anomaly_threshold: float = Field(
        default=3.0, gt=0.0, description="Z-score / distance anomaly decision threshold"
    )
    aads_decay_factor: float = Field(
        default=0.05, ge=0.0, le=1.0, description="Exponential decay factor for historical weights"
    )

    # --- Comparison Streaming Detectors (StreamAD Benchmark) ---
    xstream_num_components: int = Field(
        default=32, ge=2, description="Number of random projection components for xStream"
    )
    hstree_num_trees: int = Field(
        default=25, ge=5, description="Number of half-space trees for HSTree"
    )
    hstree_max_depth: int = Field(
        default=15, ge=3, description="Maximum tree depth for HSTree"
    )
    rrcf_num_trees: int = Field(
        default=30, ge=5, description="Number of trees in Robust Random Cut Forest"
    )
    rrcf_tree_size: int = Field(
        default=256, ge=32, description="Subsample / tree size for RRCF"
    )

    # --- LangGraph Agent & LLM ---
    llm_provider: Literal["openai", "groq", "anthropic", "mock"] = Field(
        default="openai", description="LLM provider backend"
    )
    llm_model: str = Field(
        default="gpt-4o-mini", description="Target model identifier"
    )
    llm_api_key: str = Field(
        default="", description="API key for LLM provider"
    )
    llm_base_url: str = Field(
        default="", description="Optional custom base URL for OpenAI-compatible proxies"
    )
    llm_temperature: float = Field(
        default=0.0, ge=0.0, le=1.0, description="LLM sampling temperature (deterministic = 0.0)"
    )
    llm_max_retries: int = Field(
        default=3, ge=0, description="Max retries for transient LLM call failures"
    )
    llm_timeout_seconds: float = Field(
        default=15.0, gt=0.0, description="Timeout in seconds for LLM API calls"
    )

    # --- Vector Memory Store ---
    vector_store_type: Literal["chroma", "in_memory"] = Field(
        default="chroma", description="Vector store backend for event memory"
    )
    chroma_persist_directory: Path = Field(
        default=Path("./data/chroma_db"), description="Local persistence path for Chroma DB"
    )
    memory_similarity_top_k: int = Field(
        default=5, ge=1, description="Top-k nearest historical decisions to retrieve"
    )

    # --- Database Storage ---
    database_url: str = Field(
        default="sqlite:///./data/results.db", description="SQLAlchemy connection URI"
    )

    # --- Grafana Integration ---
    grafana_port: int = Field(
        default=3000, ge=1024, le=65535, description="Grafana web dashboard port"
    )
    grafana_admin_user: str = Field(
        default="admin", description="Grafana admin username"
    )
    grafana_admin_password: str = Field(
        default="admin", description="Grafana admin password"
    )

    @field_validator("log_dir", "spark_checkpoint_dir", "chroma_persist_directory", mode="after")
    @classmethod
    def ensure_directory_exists(cls, v: Path) -> Path:
        """Ensure directories are converted to Path objects."""
        return Path(v)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Singleton getter for cached application settings."""
    return Settings()
