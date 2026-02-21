"""Pydantic models for agent configurations."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from mongoclaw.core.types import ChangeOperation, WriteStrategy


class WatchConfig(BaseModel):
    """Configuration for what MongoDB changes to watch."""

    database: str = Field(..., description="Database name to watch")
    collection: str = Field(..., description="Collection name to watch")
    operations: list[ChangeOperation] = Field(
        default=[ChangeOperation.INSERT, ChangeOperation.UPDATE],
        description="Operations to watch",
    )
    filter: dict[str, Any] | None = Field(
        default=None,
        description="MongoDB filter for matching documents",
    )
    projection: list[str] | None = Field(
        default=None,
        description="Fields to include in the document",
    )
    full_document: str = Field(
        default="updateLookup",
        description="Full document mode for change streams",
    )

    @field_validator("operations", mode="before")
    @classmethod
    def parse_operations(cls, v: Any) -> list[ChangeOperation]:
        """Parse operations from strings."""
        if isinstance(v, list):
            return [ChangeOperation(op) if isinstance(op, str) else op for op in v]
        return v


class AIConfig(BaseModel):
    """Configuration for AI provider settings."""

    provider: str = Field(
        default="openai",
        description="AI provider name (openai, anthropic, etc.)",
    )
    model: str = Field(
        default="gpt-4o-mini",
        description="Model identifier",
    )
    prompt: str = Field(
        ...,
        description="Jinja2 template for the prompt",
    )
    system_prompt: str | None = Field(
        default=None,
        description="Optional system prompt",
    )
    temperature: float = Field(
        default=0.7,
        ge=0.0,
        le=2.0,
        description="Sampling temperature",
    )
    max_tokens: int = Field(
        default=2048,
        ge=1,
        description="Maximum tokens in response",
    )
    response_schema: dict[str, Any] | None = Field(
        default=None,
        description="JSON schema for structured responses",
    )
    response_format: str | None = Field(
        default=None,
        description="Response format (json_object, etc.)",
    )
    api_key_ref: str | None = Field(
        default=None,
        description="Reference to API key in secrets backend",
    )
    extra_params: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional provider-specific parameters",
    )


class WriteConfig(BaseModel):
    """Configuration for writing results back to MongoDB."""

    strategy: WriteStrategy = Field(
        default=WriteStrategy.MERGE,
        description="Write strategy for results",
    )
    target_collection: str | None = Field(
        default=None,
        description="Target collection (defaults to source collection)",
    )
    target_database: str | None = Field(
        default=None,
        description="Target database (defaults to source database)",
    )
    fields: dict[str, str] | None = Field(
        default=None,
        description="Field mapping from AI response to document fields",
    )
    target_field: str | None = Field(
        default=None,
        description="Optional field to nest AI output under (for merge/replace)",
    )
    path: str | None = Field(
        default=None,
        description="Nested path for NESTED strategy",
    )
    array_field: str | None = Field(
        default=None,
        description="Array field name for APPEND strategy",
    )
    idempotency_key: str | None = Field(
        default=None,
        description="Jinja2 template for idempotency key",
    )
    include_metadata: bool = Field(
        default=True,
        description="Include execution metadata in written result",
    )
    metadata_field: str = Field(
        default="_ai_metadata",
        description="Field name for execution metadata",
    )

    @model_validator(mode="after")
    def validate_strategy_fields(self) -> WriteConfig:
        """Validate that required fields are present for each strategy."""
        if self.strategy == WriteStrategy.APPEND and not self.array_field:
            raise ValueError("array_field is required for APPEND strategy")
        if self.strategy == WriteStrategy.NESTED and not self.path:
            raise ValueError("path is required for NESTED strategy")
        return self


class ExecutionConfig(BaseModel):
    """Configuration for execution behavior."""

    max_retries: int = Field(
        default=3,
        ge=0,
        description="Maximum retry attempts",
    )
    retry_delay_seconds: float = Field(
        default=1.0,
        gt=0,
        description="Base delay between retries",
    )
    retry_max_delay_seconds: float = Field(
        default=60.0,
        gt=0,
        description="Maximum delay between retries",
    )
    timeout_seconds: float = Field(
        default=60.0,
        gt=0,
        description="Execution timeout",
    )
    rate_limit_requests: int | None = Field(
        default=None,
        description="Rate limit (requests per minute)",
    )
    cost_limit_usd: float | None = Field(
        default=None,
        description="Cost limit in USD per hour",
    )
    token_limit: int | None = Field(
        default=None,
        description="Token limit per hour",
    )
    priority: int = Field(
        default=0,
        ge=0,
        le=10,
        description="Execution priority (0=lowest, 10=highest)",
    )
    deduplicate: bool = Field(
        default=True,
        description="Enable deduplication of work items",
    )
    deduplicate_window_seconds: int = Field(
        default=300,
        ge=0,
        description="Deduplication window in seconds",
    )
    consistency_mode: str = Field(
        default="eventual",
        description="Consistency mode: eventual, strict_post_commit, shadow",
    )
    max_concurrency: int | None = Field(
        default=None,
        ge=1,
        description="Optional in-process per-agent concurrency cap",
    )
    require_document_hash_match: bool = Field(
        default=False,
        description="Require source document hash to match before writeback in strict mode",
    )

    @field_validator("consistency_mode")
    @classmethod
    def validate_consistency_mode(cls, v: str) -> str:
        allowed = {"eventual", "strict_post_commit", "shadow"}
        if v not in allowed:
            raise ValueError(f"consistency_mode must be one of: {', '.join(sorted(allowed))}")
        return v


class PolicyConfig(BaseModel):
    """Declarative policy configuration for execution behavior."""

    condition: str | None = Field(
        default=None,
        description="Boolean expression over document/result context",
    )
    action: Literal["enrich", "block", "tag"] = Field(
        default="enrich",
        description="Action when condition matches",
    )
    fallback_action: Literal["skip", "enrich"] = Field(
        default="skip",
        description="Action when condition does not match",
    )
    simulation_mode: bool = Field(
        default=False,
        description="If true, evaluate/log policy but skip writeback",
    )
    tag_field: str = Field(
        default="policy_tag",
        description="Field used when action=tag",
    )
    tag_value: str = Field(
        default="matched",
        description="Value written when action=tag",
    )


class AgentConfig(BaseModel):
    """Complete agent configuration."""

    id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        pattern=r"^[a-z0-9][a-z0-9_-]*[a-z0-9]$|^[a-z0-9]$",
        description="Unique agent identifier",
    )
    name: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Human-readable agent name",
    )
    description: str | None = Field(
        default=None,
        max_length=1024,
        description="Agent description",
    )
    watch: WatchConfig = Field(
        ...,
        description="Watch configuration",
    )
    ai: AIConfig = Field(
        ...,
        description="AI configuration",
    )
    write: WriteConfig = Field(
        default_factory=WriteConfig,
        description="Write configuration",
    )
    execution: ExecutionConfig = Field(
        default_factory=ExecutionConfig,
        description="Execution configuration",
    )
    policy: PolicyConfig | None = Field(
        default=None,
        description="Optional policy guardrail configuration",
    )
    enabled: bool = Field(
        default=True,
        description="Whether the agent is enabled",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Tags for organization",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Custom metadata",
    )
    version: int = Field(
        default=1,
        ge=1,
        description="Configuration version",
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="Creation timestamp",
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="Last update timestamp",
    )

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        """Validate agent ID format."""
        reserved = {"system", "admin", "root", "default", "all"}
        if v.lower() in reserved:
            raise ValueError(f"Agent ID '{v}' is reserved")
        return v.lower()

    @field_validator("tags", mode="before")
    @classmethod
    def parse_tags(cls, v: Any) -> list[str]:
        """Parse tags from comma-separated string."""
        if isinstance(v, str):
            return [t.strip() for t in v.split(",") if t.strip()]
        return v

    def model_post_init(self, __context: Any) -> None:
        """Update timestamps on modification."""
        self.updated_at = datetime.utcnow()

    def to_mongo_doc(self) -> dict[str, Any]:
        """Convert to MongoDB document format."""
        doc = self.model_dump(mode="json")
        doc["_id"] = doc.pop("id")
        return doc

    @classmethod
    def from_mongo_doc(cls, doc: dict[str, Any]) -> AgentConfig:
        """Create from MongoDB document."""
        if "_id" in doc:
            doc["id"] = doc.pop("_id")
        return cls.model_validate(doc)


class AgentSummary(BaseModel):
    """Summary view of an agent for listings."""

    id: str
    name: str
    description: str | None = None
    enabled: bool = True
    database: str
    collection: str
    operations: list[str]
    provider: str
    model: str
    tags: list[str] = Field(default_factory=list)
    version: int = 1
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_config(cls, config: AgentConfig) -> AgentSummary:
        """Create summary from full config."""
        return cls(
            id=config.id,
            name=config.name,
            description=config.description,
            enabled=config.enabled,
            database=config.watch.database,
            collection=config.watch.collection,
            operations=[op.value for op in config.watch.operations],
            provider=config.ai.provider,
            model=config.ai.model,
            tags=config.tags,
            version=config.version,
            created_at=config.created_at,
            updated_at=config.updated_at,
        )


class AgentStats(BaseModel):
    """Runtime statistics for an agent."""

    agent_id: str
    total_executions: int = 0
    successful_executions: int = 0
    failed_executions: int = 0
    total_tokens: int = 0
    total_cost_usd: float = 0.0
    avg_latency_ms: float = 0.0
    last_execution_at: datetime | None = None
    last_error: str | None = None
