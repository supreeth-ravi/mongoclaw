"""Protocols and type definitions for MongoClaw."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from mongoclaw.agents.models import AgentConfig
    from mongoclaw.dispatcher.work_item import WorkItem


class ChangeOperation(str, Enum):
    """MongoDB change stream operations."""

    INSERT = "insert"
    UPDATE = "update"
    REPLACE = "replace"
    DELETE = "delete"


class WriteStrategy(str, Enum):
    """Strategies for writing AI results back to MongoDB."""

    MERGE = "merge"  # Merge into existing document
    REPLACE = "replace"  # Replace specified fields
    APPEND = "append"  # Append to array field
    NESTED = "nested"  # Write to nested path


class AIProvider(str, Enum):
    """Supported AI providers."""

    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    AZURE = "azure"
    BEDROCK = "bedrock"
    COHERE = "cohere"
    GOOGLE = "google"
    GROQ = "groq"
    MISTRAL = "mistral"
    OLLAMA = "ollama"
    TOGETHER = "together"
    CUSTOM = "custom"


class ExecutionStatus(str, Enum):
    """Execution status for work items."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"
    CANCELLED = "cancelled"


class HealthStatus(str, Enum):
    """Health check status."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


# Type aliases for common structures
DocumentId = str
AgentId = str
WorkItemId = str
ResumeToken = dict[str, Any]
JsonSchema = dict[str, Any]


class ChangeEvent:
    """Represents a MongoDB change stream event."""

    def __init__(
        self,
        operation: ChangeOperation,
        database: str,
        collection: str,
        document_key: dict[str, Any],
        full_document: dict[str, Any] | None = None,
        update_description: dict[str, Any] | None = None,
        resume_token: ResumeToken | None = None,
        cluster_time: datetime | None = None,
        wall_time: datetime | None = None,
    ) -> None:
        self.operation = operation
        self.database = database
        self.collection = collection
        self.document_key = document_key
        self.full_document = full_document
        self.update_description = update_description
        self.resume_token = resume_token
        self.cluster_time = cluster_time
        self.wall_time = wall_time or datetime.utcnow()

    @property
    def document_id(self) -> str:
        """Get the document _id as string."""
        doc_id = self.document_key.get("_id")
        return str(doc_id) if doc_id else ""

    @property
    def namespace(self) -> str:
        """Get the full namespace (database.collection)."""
        return f"{self.database}.{self.collection}"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "operation": self.operation.value,
            "database": self.database,
            "collection": self.collection,
            "document_key": self.document_key,
            "full_document": self.full_document,
            "update_description": self.update_description,
            "resume_token": self.resume_token,
            "cluster_time": self.cluster_time.isoformat() if self.cluster_time else None,
            "wall_time": self.wall_time.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ChangeEvent:
        """Create from dictionary."""
        return cls(
            operation=ChangeOperation(data["operation"]),
            database=data["database"],
            collection=data["collection"],
            document_key=data["document_key"],
            full_document=data.get("full_document"),
            update_description=data.get("update_description"),
            resume_token=data.get("resume_token"),
            cluster_time=(
                datetime.fromisoformat(data["cluster_time"])
                if data.get("cluster_time")
                else None
            ),
            wall_time=(
                datetime.fromisoformat(data["wall_time"])
                if data.get("wall_time")
                else None
            ),
        )


class AIResponse:
    """Represents a response from an AI provider."""

    def __init__(
        self,
        content: str,
        parsed_content: dict[str, Any] | None = None,
        model: str = "",
        provider: str = "",
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: int = 0,
        cost_usd: float = 0.0,
        latency_ms: float = 0.0,
        finish_reason: str = "",
        raw_response: dict[str, Any] | None = None,
    ) -> None:
        self.content = content
        self.parsed_content = parsed_content
        self.model = model
        self.provider = provider
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.total_tokens = total_tokens
        self.cost_usd = cost_usd
        self.latency_ms = latency_ms
        self.finish_reason = finish_reason
        self.raw_response = raw_response

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "content": self.content,
            "parsed_content": self.parsed_content,
            "model": self.model,
            "provider": self.provider,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "cost_usd": self.cost_usd,
            "latency_ms": self.latency_ms,
            "finish_reason": self.finish_reason,
        }


@runtime_checkable
class QueueBackend(Protocol):
    """Protocol for queue backend implementations."""

    async def connect(self) -> None:
        """Connect to the queue backend."""
        ...

    async def disconnect(self) -> None:
        """Disconnect from the queue backend."""
        ...

    async def enqueue(self, work_item: WorkItem, stream_name: str) -> str:
        """
        Enqueue a work item.

        Args:
            work_item: The work item to enqueue.
            stream_name: The stream/queue name.

        Returns:
            The message ID.
        """
        ...

    async def dequeue(
        self,
        stream_name: str,
        consumer_group: str,
        consumer_name: str,
        count: int = 1,
        block_ms: int = 5000,
    ) -> list[tuple[str, WorkItem]]:
        """
        Dequeue work items.

        Args:
            stream_name: The stream/queue name.
            consumer_group: The consumer group name.
            consumer_name: The consumer name.
            count: Maximum number of items to dequeue.
            block_ms: Block time in milliseconds.

        Returns:
            List of (message_id, work_item) tuples.
        """
        ...

    async def ack(self, stream_name: str, consumer_group: str, message_id: str) -> None:
        """
        Acknowledge a message.

        Args:
            stream_name: The stream/queue name.
            consumer_group: The consumer group name.
            message_id: The message ID to acknowledge.
        """
        ...

    async def move_to_dlq(
        self,
        work_item: WorkItem,
        error: Exception,
        dlq_stream: str,
    ) -> str:
        """
        Move a work item to the dead letter queue.

        Args:
            work_item: The work item to move.
            error: The error that caused the move.
            dlq_stream: The dead letter queue stream name.

        Returns:
            The DLQ message ID.
        """
        ...

    async def get_pending_count(self, stream_name: str, consumer_group: str) -> int:
        """Get the count of pending messages."""
        ...

    async def get_stream_length(self, stream_name: str) -> int:
        """Get the length of a stream."""
        ...


@runtime_checkable
class SecretsBackend(Protocol):
    """Protocol for secrets backend implementations."""

    async def get_secret(self, reference: str) -> str:
        """
        Retrieve a secret value.

        Args:
            reference: The secret reference (format varies by backend).

        Returns:
            The secret value.

        Raises:
            SecretNotFoundError: If the secret is not found.
        """
        ...

    async def health_check(self) -> bool:
        """Check if the secrets backend is healthy."""
        ...


@runtime_checkable
class ResultWriter(Protocol):
    """Protocol for writing results back to MongoDB."""

    async def write(
        self,
        collection: str,
        document_id: str,
        result: dict[str, Any],
        strategy: WriteStrategy,
        idempotency_key: str | None = None,
    ) -> bool:
        """
        Write a result to MongoDB.

        Args:
            collection: The collection name.
            document_id: The document ID.
            result: The result data to write.
            strategy: The write strategy.
            idempotency_key: Optional idempotency key.

        Returns:
            True if write was successful.
        """
        ...


@runtime_checkable
class AgentMatcher(Protocol):
    """Protocol for matching change events to agents."""

    async def match(self, event: ChangeEvent) -> list[AgentConfig]:
        """
        Find all agents that match a change event.

        Args:
            event: The change event to match.

        Returns:
            List of matching agent configurations.
        """
        ...


@runtime_checkable
class ExecutionContext(Protocol):
    """Protocol for execution context passed to workers."""

    @property
    def agent_id(self) -> str:
        """Get the agent ID."""
        ...

    @property
    def work_item_id(self) -> str:
        """Get the work item ID."""
        ...

    @property
    def attempt(self) -> int:
        """Get the current attempt number."""
        ...

    @property
    def start_time(self) -> datetime:
        """Get the execution start time."""
        ...

    @property
    def document(self) -> dict[str, Any]:
        """Get the document being processed."""
        ...


class ExecutionResult:
    """Result of an agent execution."""

    def __init__(
        self,
        success: bool,
        agent_id: str,
        work_item_id: str,
        ai_response: AIResponse | None = None,
        written: bool = False,
        error: Exception | None = None,
        duration_ms: float = 0.0,
        attempt: int = 1,
    ) -> None:
        self.success = success
        self.agent_id = agent_id
        self.work_item_id = work_item_id
        self.ai_response = ai_response
        self.written = written
        self.error = error
        self.duration_ms = duration_ms
        self.attempt = attempt

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "agent_id": self.agent_id,
            "work_item_id": self.work_item_id,
            "ai_response": self.ai_response.to_dict() if self.ai_response else None,
            "written": self.written,
            "error": str(self.error) if self.error else None,
            "duration_ms": self.duration_ms,
            "attempt": self.attempt,
        }


class HealthCheckResult:
    """Result of a health check."""

    def __init__(
        self,
        component: str,
        status: HealthStatus,
        message: str = "",
        latency_ms: float = 0.0,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.component = component
        self.status = status
        self.message = message
        self.latency_ms = latency_ms
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "component": self.component,
            "status": self.status.value,
            "message": self.message,
            "latency_ms": self.latency_ms,
            "details": self.details,
        }
