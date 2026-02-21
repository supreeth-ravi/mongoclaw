"""Work item model for queue messages."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime
from typing import Any

from bson import ObjectId
from bson.timestamp import Timestamp
from pydantic import BaseModel, Field

from mongoclaw.core.types import ChangeEvent


def _make_serializable(obj: Any) -> Any:
    """Convert MongoDB types to JSON-serializable types."""
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, Timestamp):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_make_serializable(item) for item in obj]
    return obj


class WorkItem(BaseModel):
    """
    Represents a unit of work to be processed by a worker.

    Contains all information needed to execute an agent on a document.
    """

    id: str = Field(
        default_factory=lambda: uuid.uuid4().hex,
        description="Unique work item ID",
    )
    agent_id: str = Field(
        ...,
        description="Agent configuration ID",
    )
    change_event: dict[str, Any] = Field(
        ...,
        description="Serialized change event",
    )
    document: dict[str, Any] = Field(
        default_factory=dict,
        description="The document to process",
    )
    document_id: str = Field(
        default="",
        description="Document _id as string",
    )
    source_version: int | None = Field(
        default=None,
        description="Observed _mongoclaw_version at dispatch time",
    )
    source_document_hash: str | None = Field(
        default=None,
        description="Dispatch-time hash of the source document",
    )
    database: str = Field(
        default="",
        description="Source database",
    )
    collection: str = Field(
        default="",
        description="Source collection",
    )
    attempt: int = Field(
        default=0,
        ge=0,
        description="Current attempt number",
    )
    max_attempts: int = Field(
        default=3,
        ge=1,
        description="Maximum attempts before DLQ",
    )
    priority: int = Field(
        default=0,
        ge=0,
        le=10,
        description="Processing priority",
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="Creation timestamp",
    )
    scheduled_at: datetime | None = Field(
        default=None,
        description="Scheduled processing time (for delayed retries)",
    )
    idempotency_key: str | None = Field(
        default=None,
        description="Key for deduplication",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata",
    )
    trace_id: str | None = Field(
        default=None,
        description="Trace ID for distributed tracing",
    )
    parent_span_id: str | None = Field(
        default=None,
        description="Parent span ID for tracing",
    )

    @classmethod
    def from_event(
        cls,
        agent_id: str,
        event: ChangeEvent,
        max_attempts: int = 3,
        priority: int = 0,
        idempotency_key: str | None = None,
    ) -> WorkItem:
        """
        Create a work item from a change event.

        Args:
            agent_id: The agent ID to process with.
            event: The change event.
            max_attempts: Maximum retry attempts.
            priority: Processing priority.
            idempotency_key: Optional idempotency key.

        Returns:
            A new work item.
        """
        return cls(
            agent_id=agent_id,
            change_event=_make_serializable(event.to_dict()),
            document=_make_serializable(event.full_document or {}),
            document_id=str(event.document_id) if event.document_id else "",
            source_version=_extract_source_version(event.full_document),
            source_document_hash=_extract_source_document_hash(event.full_document),
            database=event.database,
            collection=event.collection,
            max_attempts=max_attempts,
            priority=priority,
            idempotency_key=idempotency_key,
        )

    def get_change_event(self) -> ChangeEvent:
        """Deserialize the change event."""
        return ChangeEvent.from_dict(self.change_event)

    def increment_attempt(self) -> WorkItem:
        """Create a copy with incremented attempt count."""
        return self.model_copy(update={"attempt": self.attempt + 1})

    def should_retry(self) -> bool:
        """Check if this item should be retried."""
        return self.attempt < self.max_attempts

    def generate_idempotency_key(self) -> str:
        """
        Generate an idempotency key based on content.

        The key is based on agent_id, document_id, and document hash.
        """
        doc_hash = hashlib.md5(
            str(sorted(self.document.items())).encode()
        ).hexdigest()[:8]

        return f"{self.agent_id}:{self.document_id}:{doc_hash}"

    def to_queue_data(self) -> dict[str, Any]:
        """
        Convert to data suitable for queue serialization.

        Returns:
            Dictionary for queue storage.
        """
        return self.model_dump(mode="json")

    @classmethod
    def from_queue_data(cls, data: dict[str, Any]) -> WorkItem:
        """
        Create from queue data.

        Args:
            data: Dictionary from queue.

        Returns:
            WorkItem instance.
        """
        return cls.model_validate(data)


class WorkItemResult(BaseModel):
    """Result of processing a work item."""

    work_item_id: str
    agent_id: str
    success: bool
    ai_response: dict[str, Any] | None = None
    written: bool = False
    lifecycle_state: str = "completed"
    reason: str = "completed"
    error: str | None = None
    error_type: str | None = None
    duration_ms: float = 0.0
    attempt: int = 0
    completed_at: datetime = Field(default_factory=datetime.utcnow)

    @classmethod
    def success_result(
        cls,
        work_item: WorkItem,
        ai_response: dict[str, Any] | None = None,
        written: bool = False,
        duration_ms: float = 0.0,
        lifecycle_state: str = "completed",
        reason: str = "completed",
    ) -> WorkItemResult:
        """Create a success result."""
        return cls(
            work_item_id=work_item.id,
            agent_id=work_item.agent_id,
            success=True,
            ai_response=ai_response,
            written=written,
            duration_ms=duration_ms,
            attempt=work_item.attempt,
            lifecycle_state=lifecycle_state,
            reason=reason,
        )

    @classmethod
    def failure_result(
        cls,
        work_item: WorkItem,
        error: Exception,
        duration_ms: float = 0.0,
        lifecycle_state: str = "failed",
        reason: str = "failed",
    ) -> WorkItemResult:
        """Create a failure result."""
        return cls(
            work_item_id=work_item.id,
            agent_id=work_item.agent_id,
            success=False,
            error=str(error),
            error_type=type(error).__name__,
            duration_ms=duration_ms,
            attempt=work_item.attempt,
            lifecycle_state=lifecycle_state,
            reason=reason,
        )


def _extract_source_version(full_document: dict[str, Any] | None) -> int | None:
    """Extract dispatch-time version stamp from a full document."""
    if not isinstance(full_document, dict):
        return None

    raw = full_document.get("_mongoclaw_version")
    if raw is None:
        return 0
    if isinstance(raw, int):
        return raw
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _extract_source_document_hash(full_document: dict[str, Any] | None) -> str | None:
    """Create dispatch-time source hash used by optional strict guard."""
    if not isinstance(full_document, dict):
        return None
    normalized = _normalize_for_hash(_make_serializable(full_document))
    serialized = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()


def _normalize_for_hash(document: dict[str, Any]) -> dict[str, Any]:
    """Normalize document for stable hashing."""
    ignored = {"_ai_metadata", "_mongoclaw_version"}
    normalized: dict[str, Any] = {}
    for key, value in document.items():
        if key in ignored:
            continue
        normalized[key] = _normalize_value(value)
    return normalized


def _normalize_value(value: Any) -> Any:
    """Normalize values recursively for hashing."""
    if isinstance(value, dict):
        return _normalize_for_hash(value)
    if isinstance(value, list):
        return [_normalize_value(v) for v in value]
    return value
