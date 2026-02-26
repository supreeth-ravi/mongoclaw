"""Configuration management using Pydantic settings."""

from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LogLevel(str, Enum):
    """Log level options."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class SecretsBackendType(str, Enum):
    """Secrets backend types."""

    ENV = "env"
    VAULT = "vault"
    AWS = "aws"


class MongoDBSettings(BaseSettings):
    """MongoDB connection settings."""

    model_config = SettingsConfigDict(env_prefix="MONGODB_")

    uri: SecretStr = Field(
        default=SecretStr("mongodb://localhost:27017"),
        description="MongoDB connection URI",
    )
    database: str = Field(
        default="mongoclaw",
        description="Default database name for agent configs",
    )
    agents_collection: str = Field(
        default="agents",
        description="Collection name for agent configurations",
    )
    executions_collection: str = Field(
        default="executions",
        description="Collection name for execution history",
    )
    resume_tokens_collection: str = Field(
        default="resume_tokens",
        description="Collection for change stream resume tokens",
    )
    max_pool_size: int = Field(default=100, ge=1)
    min_pool_size: int = Field(default=10, ge=1)
    server_selection_timeout_ms: int = Field(default=5000, ge=1000)


class RedisSettings(BaseSettings):
    """Redis connection settings."""

    model_config = SettingsConfigDict(env_prefix="REDIS_")

    url: SecretStr = Field(
        default=SecretStr("redis://localhost:6379/0"),
        description="Redis connection URL",
    )
    max_connections: int = Field(default=50, ge=1)
    socket_timeout: float = Field(default=5.0, gt=0)
    socket_connect_timeout: float = Field(default=5.0, gt=0)
    retry_on_timeout: bool = Field(default=True)
    stream_max_len: int = Field(
        default=100000,
        description="Maximum length of Redis streams before trimming",
    )
    consumer_group: str = Field(
        default="mongoclaw-workers",
        description="Consumer group name for Redis streams",
    )
    block_ms: int = Field(
        default=5000,
        description="Block time in ms when reading from streams",
    )


class AISettings(BaseSettings):
    """AI provider settings."""

    model_config = SettingsConfigDict(env_prefix="AI_")

    default_provider: str = Field(
        default="openai",
        description="Default AI provider to use",
    )
    default_model: str = Field(
        default="gpt-4o-mini",
        description="Default model to use",
    )
    default_temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    default_max_tokens: int = Field(default=2048, ge=1)
    request_timeout: float = Field(default=60.0, gt=0)
    max_retries: int = Field(default=3, ge=0)
    cache_enabled: bool = Field(default=True)
    cache_ttl_seconds: int = Field(default=3600)
    global_cost_limit_usd: float | None = Field(
        default=None,
        description="Global cost limit in USD per day",
    )
    global_token_limit: int | None = Field(
        default=None,
        description="Global token limit per day",
    )


class WorkerSettings(BaseSettings):
    """Worker pool settings."""

    model_config = SettingsConfigDict(env_prefix="WORKER_")

    pool_size: int = Field(
        default=10,
        ge=1,
        description="Number of concurrent workers",
    )
    routing_strategy: Literal[
        "by_agent",
        "by_collection",
        "single",
        "partitioned",
        "by_priority",
    ] = Field(
        default="by_agent",
        description="Routing strategy for queue stream assignment",
    )
    routing_partition_count: int = Field(
        default=8,
        ge=1,
        description="Number of partitions when routing_strategy=partitioned",
    )
    batch_size: int = Field(
        default=10,
        ge=1,
        description="Number of items to dequeue at once",
    )
    fair_scheduling_enabled: bool = Field(
        default=True,
        description="Enable rotating fair scheduling across streams",
    )
    fair_stream_batch_size: int = Field(
        default=1,
        ge=1,
        description="Items to pull per stream when fair scheduling is enabled",
    )
    fair_streams_per_cycle: int | None = Field(
        default=None,
        ge=1,
        description="Optional cap on streams processed per worker cycle",
    )
    max_in_flight_per_agent_stream: int | None = Field(
        default=None,
        ge=1,
        description="Optional global in-flight cap per agent stream across workers",
    )
    pending_metrics_interval_seconds: float = Field(
        default=10.0,
        gt=0,
        description="How often to sample per-agent stream pending depth",
    )
    starvation_cycle_threshold: int = Field(
        default=20,
        ge=1,
        description="Consecutive empty cycles before emitting starvation signal",
    )
    dispatch_backpressure_enabled: bool = Field(
        default=True,
        description="Enable queue-pressure admission control at dispatch time",
    )
    dispatch_backpressure_threshold: float = Field(
        default=0.8,
        ge=0.0,
        le=1.0,
        description="Queue fullness threshold for dispatch backpressure actions",
    )
    dispatch_overflow_policy: Literal["drop", "defer", "dlq"] = Field(
        default="defer",
        description="Overflow behavior under dispatch backpressure",
    )
    dispatch_min_priority_when_backpressured: int = Field(
        default=5,
        ge=0,
        le=10,
        description="Minimum priority allowed to bypass backpressure",
    )
    dispatch_defer_seconds: float = Field(
        default=0.25,
        gt=0,
        description="Delay between deferred admission checks",
    )
    dispatch_defer_max_attempts: int = Field(
        default=3,
        ge=1,
        description="Maximum defer checks before forced enqueue",
    )
    dispatch_pressure_cache_ttl_seconds: float = Field(
        default=1.0,
        gt=0,
        description="TTL for stream pressure sampling cache",
    )
    agent_error_budget_window_seconds: float = Field(
        default=60.0,
        gt=0,
        description="Window for per-agent failure budget accounting",
    )
    agent_error_budget_max_failures: int = Field(
        default=20,
        ge=1,
        description="Failure count in window before temporary quarantine",
    )
    agent_quarantine_seconds: float = Field(
        default=30.0,
        gt=0,
        description="Temporary quarantine duration after budget exhaustion",
    )
    latency_slo_ms: float = Field(
        default=3000.0,
        gt=0,
        description="Latency SLO threshold in milliseconds",
    )
    max_retries: int = Field(default=3, ge=0)
    retry_base_delay: float = Field(default=1.0, gt=0)
    retry_max_delay: float = Field(default=60.0, gt=0)
    execution_timeout: float = Field(default=300.0, gt=0)
    shutdown_timeout: float = Field(default=30.0, gt=0)
    backpressure_threshold: float = Field(
        default=0.8,
        ge=0.0,
        le=1.0,
        description="Queue fullness threshold to trigger backpressure",
    )


class APISettings(BaseSettings):
    """API server settings."""

    model_config = SettingsConfigDict(env_prefix="API_")

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000, ge=1, le=65535)
    workers: int = Field(default=1, ge=1)
    reload: bool = Field(default=False)
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])
    api_key_header: str = Field(default="X-API-Key")
    rate_limit_requests: int = Field(default=100)
    rate_limit_window_seconds: int = Field(default=60)


class SecuritySettings(BaseSettings):
    """Security settings."""

    model_config = SettingsConfigDict(env_prefix="SECURITY_")

    secrets_backend: SecretsBackendType = Field(default=SecretsBackendType.ENV)
    vault_url: str | None = Field(default=None)
    vault_token: SecretStr | None = Field(default=None)
    vault_mount_point: str = Field(default="secret")
    aws_region: str = Field(default="us-east-1")
    pii_redaction_enabled: bool = Field(default=True)
    audit_logging_enabled: bool = Field(default=True)
    api_keys: list[SecretStr] = Field(default_factory=list)

    @field_validator("api_keys", mode="before")
    @classmethod
    def parse_api_keys(cls, v: str | list[str] | list[SecretStr]) -> list[SecretStr]:
        """Parse API keys from comma-separated string or list."""
        if isinstance(v, str):
            return [SecretStr(k.strip()) for k in v.split(",") if k.strip()]
        return [SecretStr(k) if isinstance(k, str) else k for k in v]


class ObservabilitySettings(BaseSettings):
    """Observability settings."""

    model_config = SettingsConfigDict(env_prefix="OBSERVABILITY_")

    log_level: LogLevel = Field(default=LogLevel.INFO)
    log_format: Literal["json", "console"] = Field(default="json")
    metrics_enabled: bool = Field(default=True)
    metrics_port: int = Field(default=9090, ge=1, le=65535)
    tracing_enabled: bool = Field(default=False)
    tracing_endpoint: str | None = Field(default=None)
    tracing_sample_rate: float = Field(default=0.1, ge=0.0, le=1.0)
    service_name: str = Field(default="mongoclaw")


class Settings(BaseSettings):
    """Main application settings."""

    model_config = SettingsConfigDict(
        env_prefix="MONGOCLAW_",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    # Environment
    environment: Literal["development", "staging", "production"] = Field(
        default="development"
    )
    debug: bool = Field(default=False)

    # Component settings
    mongodb: MongoDBSettings = Field(default_factory=MongoDBSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    ai: AISettings = Field(default_factory=AISettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    api: APISettings = Field(default_factory=APISettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)

    # Feature flags
    leader_election_enabled: bool = Field(
        default=True,
        description="Enable leader election for change stream watchers",
    )
    hot_reload_enabled: bool = Field(
        default=True,
        description="Enable hot reload of agent configurations",
    )

    @classmethod
    def load(cls) -> Settings:
        """Load settings from environment."""
        return cls()


# Global settings instance (lazy loaded)
_settings: Settings | None = None


def get_settings() -> Settings:
    """Get the global settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings.load()
    return _settings


def configure_settings(settings: Settings) -> None:
    """Configure the global settings instance (for testing)."""
    global _settings
    _settings = settings
