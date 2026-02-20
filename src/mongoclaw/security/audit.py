"""Audit logging for security events."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)


class AuditAction(str, Enum):
    """Auditable actions."""

    # Authentication
    AUTH_SUCCESS = "auth.success"
    AUTH_FAILURE = "auth.failure"
    AUTH_LOGOUT = "auth.logout"

    # Agent management
    AGENT_CREATED = "agent.created"
    AGENT_UPDATED = "agent.updated"
    AGENT_DELETED = "agent.deleted"
    AGENT_ENABLED = "agent.enabled"
    AGENT_DISABLED = "agent.disabled"

    # Execution
    EXECUTION_STARTED = "execution.started"
    EXECUTION_COMPLETED = "execution.completed"
    EXECUTION_FAILED = "execution.failed"
    EXECUTION_RETRIED = "execution.retried"

    # Queue operations
    QUEUE_PURGED = "queue.purged"
    DLQ_ITEM_RETRIED = "dlq.item_retried"
    DLQ_ITEM_DELETED = "dlq.item_deleted"

    # System
    CONFIG_CHANGED = "config.changed"
    SYSTEM_SHUTDOWN = "system.shutdown"
    SYSTEM_STARTUP = "system.startup"

    # Security
    SECRET_ACCESSED = "secret.accessed"
    PII_DETECTED = "pii.detected"
    PERMISSION_DENIED = "permission.denied"


class AuditEvent(BaseModel):
    """Audit event model."""

    timestamp: datetime = Field(default_factory=datetime.utcnow)
    action: AuditAction
    actor: str | None = None  # User or system component
    resource_type: str | None = None
    resource_id: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)
    ip_address: str | None = None
    user_agent: str | None = None
    success: bool = True
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "action": self.action.value,
            "actor": self.actor,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "details": self.details,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "success": self.success,
            "error": self.error,
        }


class AuditLogger:
    """
    Audit logger for recording security-relevant events.

    Supports:
    - Structured logging
    - MongoDB storage
    - Event filtering
    """

    def __init__(
        self,
        enabled: bool = True,
        log_to_stdout: bool = True,
        store: Any = None,  # MongoDB collection or other store
    ) -> None:
        """
        Initialize audit logger.

        Args:
            enabled: Whether audit logging is enabled.
            log_to_stdout: Also log to stdout.
            store: Optional storage backend.
        """
        self._enabled = enabled
        self._log_to_stdout = log_to_stdout
        self._store = store

    @property
    def enabled(self) -> bool:
        """Check if audit logging is enabled."""
        return self._enabled

    async def log(self, event: AuditEvent) -> None:
        """
        Log an audit event.

        Args:
            event: The audit event to log.
        """
        if not self._enabled:
            return

        # Log to stdout
        if self._log_to_stdout:
            log_method = logger.info if event.success else logger.warning
            log_method(
                "Audit event",
                action=event.action.value,
                actor=event.actor,
                resource_type=event.resource_type,
                resource_id=event.resource_id,
                success=event.success,
                error=event.error,
            )

        # Store in database
        if self._store is not None:
            try:
                await self._store.insert_one(event.to_dict())
            except Exception as e:
                logger.error("Failed to store audit event", error=str(e))

    async def log_action(
        self,
        action: AuditAction,
        actor: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        success: bool = True,
        error: str | None = None,
        **details: Any,
    ) -> None:
        """
        Log an action with simplified parameters.

        Args:
            action: The action performed.
            actor: Who performed the action.
            resource_type: Type of resource affected.
            resource_id: ID of resource affected.
            success: Whether action succeeded.
            error: Error message if failed.
            **details: Additional details.
        """
        event = AuditEvent(
            action=action,
            actor=actor,
            resource_type=resource_type,
            resource_id=resource_id,
            success=success,
            error=error,
            details=details,
        )
        await self.log(event)

    # Convenience methods for common events

    async def log_auth_success(
        self,
        user_id: str,
        ip_address: str | None = None,
    ) -> None:
        """Log successful authentication."""
        await self.log_action(
            action=AuditAction.AUTH_SUCCESS,
            actor=user_id,
            ip_address=ip_address,
        )

    async def log_auth_failure(
        self,
        user_id: str | None = None,
        reason: str = "Invalid credentials",
        ip_address: str | None = None,
    ) -> None:
        """Log failed authentication."""
        await self.log_action(
            action=AuditAction.AUTH_FAILURE,
            actor=user_id,
            success=False,
            error=reason,
            ip_address=ip_address,
        )

    async def log_agent_change(
        self,
        action: AuditAction,
        agent_id: str,
        actor: str | None = None,
        **details: Any,
    ) -> None:
        """Log agent configuration change."""
        await self.log_action(
            action=action,
            actor=actor,
            resource_type="agent",
            resource_id=agent_id,
            **details,
        )

    async def log_permission_denied(
        self,
        user_id: str,
        permission: str,
        resource: str | None = None,
    ) -> None:
        """Log permission denied event."""
        await self.log_action(
            action=AuditAction.PERMISSION_DENIED,
            actor=user_id,
            resource_type="permission",
            resource_id=resource,
            success=False,
            permission=permission,
        )

    async def query(
        self,
        action: AuditAction | None = None,
        actor: str | None = None,
        resource_type: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Query audit events.

        Args:
            action: Filter by action.
            actor: Filter by actor.
            resource_type: Filter by resource type.
            start_time: Filter by start time.
            end_time: Filter by end time.
            limit: Maximum results.

        Returns:
            List of matching events.
        """
        if self._store is None:
            return []

        query: dict[str, Any] = {}

        if action:
            query["action"] = action.value
        if actor:
            query["actor"] = actor
        if resource_type:
            query["resource_type"] = resource_type

        if start_time or end_time:
            query["timestamp"] = {}
            if start_time:
                query["timestamp"]["$gte"] = start_time.isoformat()
            if end_time:
                query["timestamp"]["$lte"] = end_time.isoformat()

        try:
            cursor = self._store.find(query).sort("timestamp", -1).limit(limit)
            return [doc async for doc in cursor]
        except Exception as e:
            logger.error("Failed to query audit events", error=str(e))
            return []

    def set_enabled(self, enabled: bool) -> None:
        """Enable or disable audit logging."""
        self._enabled = enabled
