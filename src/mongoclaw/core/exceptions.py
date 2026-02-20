"""Exception hierarchy for MongoClaw."""

from __future__ import annotations

from typing import Any


class MongoClawError(Exception):
    """Base exception for all MongoClaw errors."""

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} - {self.details}"
        return self.message


# Configuration Errors
class ConfigurationError(MongoClawError):
    """Error in configuration."""

    pass


class ValidationError(ConfigurationError):
    """Validation error for agent configs or data."""

    pass


class SecretNotFoundError(ConfigurationError):
    """Secret reference could not be resolved."""

    def __init__(self, reference: str, backend: str) -> None:
        super().__init__(
            f"Secret '{reference}' not found in backend '{backend}'",
            {"reference": reference, "backend": backend},
        )


# Agent Errors
class AgentError(MongoClawError):
    """Base error for agent-related issues."""

    def __init__(
        self, message: str, agent_id: str, details: dict[str, Any] | None = None
    ) -> None:
        super().__init__(message, details)
        self.agent_id = agent_id


class AgentNotFoundError(AgentError):
    """Agent configuration not found."""

    def __init__(self, agent_id: str) -> None:
        super().__init__(f"Agent '{agent_id}' not found", agent_id)


class AgentDisabledError(AgentError):
    """Agent is disabled."""

    def __init__(self, agent_id: str) -> None:
        super().__init__(f"Agent '{agent_id}' is disabled", agent_id)


class AgentConfigError(AgentError):
    """Error in agent configuration."""

    pass


# Execution Errors
class ExecutionError(MongoClawError):
    """Base error for execution-related issues."""

    def __init__(
        self,
        message: str,
        agent_id: str,
        work_item_id: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, details)
        self.agent_id = agent_id
        self.work_item_id = work_item_id


class ExecutionTimeoutError(ExecutionError):
    """Execution exceeded timeout."""

    def __init__(self, agent_id: str, work_item_id: str, timeout_seconds: float) -> None:
        super().__init__(
            f"Execution timed out after {timeout_seconds}s",
            agent_id,
            work_item_id,
            {"timeout_seconds": timeout_seconds},
        )


class MaxRetriesExceededError(ExecutionError):
    """Maximum retry attempts exceeded."""

    def __init__(
        self,
        agent_id: str,
        work_item_id: str,
        max_retries: int,
        last_error: Exception | None = None,
    ) -> None:
        details: dict[str, Any] = {"max_retries": max_retries}
        if last_error:
            details["last_error"] = str(last_error)
        super().__init__(
            f"Max retries ({max_retries}) exceeded",
            agent_id,
            work_item_id,
            details,
        )
        self.last_error = last_error


# AI Errors
class AIError(MongoClawError):
    """Base error for AI-related issues."""

    def __init__(
        self,
        message: str,
        provider: str | None = None,
        model: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        all_details = details or {}
        if provider:
            all_details["provider"] = provider
        if model:
            all_details["model"] = model
        super().__init__(message, all_details)
        self.provider = provider
        self.model = model


class AIProviderError(AIError):
    """Error from AI provider."""

    pass


class AIRateLimitError(AIError):
    """Rate limit exceeded for AI provider."""

    def __init__(
        self,
        provider: str,
        model: str | None = None,
        retry_after: float | None = None,
    ) -> None:
        details: dict[str, Any] = {}
        if retry_after:
            details["retry_after_seconds"] = retry_after
        super().__init__(
            f"Rate limit exceeded for provider '{provider}'",
            provider,
            model,
            details,
        )
        self.retry_after = retry_after


class AIResponseParseError(AIError):
    """Failed to parse AI response."""

    def __init__(
        self,
        message: str,
        raw_response: str | None = None,
        provider: str | None = None,
        model: str | None = None,
    ) -> None:
        details: dict[str, Any] = {}
        if raw_response:
            details["raw_response"] = raw_response[:500]  # Truncate for logging
        super().__init__(message, provider, model, details)


class CostLimitExceededError(AIError):
    """Cost or token limit exceeded."""

    def __init__(
        self,
        limit_type: str,
        current_value: float,
        limit_value: float,
        agent_id: str | None = None,
    ) -> None:
        details = {
            "limit_type": limit_type,
            "current_value": current_value,
            "limit_value": limit_value,
        }
        if agent_id:
            details["agent_id"] = agent_id
        super().__init__(
            f"{limit_type} limit exceeded: {current_value} >= {limit_value}",
            details=details,
        )


class PromptRenderError(AIError):
    """Error rendering prompt template."""

    def __init__(self, template_error: str, template_name: str | None = None) -> None:
        details = {"template_error": template_error}
        if template_name:
            details["template_name"] = template_name
        super().__init__(f"Failed to render prompt: {template_error}", details=details)


# Queue Errors
class QueueError(MongoClawError):
    """Base error for queue-related issues."""

    pass


class QueueConnectionError(QueueError):
    """Failed to connect to queue backend."""

    pass


class QueueFullError(QueueError):
    """Queue is full, backpressure needed."""

    def __init__(self, queue_name: str, current_size: int, max_size: int) -> None:
        super().__init__(
            f"Queue '{queue_name}' is full",
            {
                "queue_name": queue_name,
                "current_size": current_size,
                "max_size": max_size,
            },
        )


class DeadLetterError(QueueError):
    """Error moving item to dead letter queue."""

    pass


# Database Errors
class DatabaseError(MongoClawError):
    """Base error for database operations."""

    pass


class ConnectionError(DatabaseError):
    """Failed to connect to database."""

    pass


class IdempotencyError(DatabaseError):
    """Idempotency check failed."""

    def __init__(self, idempotency_key: str, operation: str) -> None:
        super().__init__(
            f"Idempotency violation for key '{idempotency_key}'",
            {"idempotency_key": idempotency_key, "operation": operation},
        )


class WriteConflictError(DatabaseError):
    """Write conflict detected."""

    def __init__(self, collection: str, document_id: str) -> None:
        super().__init__(
            f"Write conflict in '{collection}' for document '{document_id}'",
            {"collection": collection, "document_id": document_id},
        )


# Resilience Errors
class CircuitBreakerOpenError(MongoClawError):
    """Circuit breaker is open."""

    def __init__(self, circuit_name: str, failure_count: int) -> None:
        super().__init__(
            f"Circuit breaker '{circuit_name}' is open",
            {"circuit_name": circuit_name, "failure_count": failure_count},
        )


class HealthCheckError(MongoClawError):
    """Health check failed."""

    def __init__(self, component: str, reason: str) -> None:
        super().__init__(
            f"Health check failed for '{component}': {reason}",
            {"component": component, "reason": reason},
        )


# Security Errors
class SecurityError(MongoClawError):
    """Base error for security issues."""

    pass


class AuthenticationError(SecurityError):
    """Authentication failed."""

    pass


class AuthorizationError(SecurityError):
    """Authorization denied."""

    def __init__(self, action: str, resource: str, role: str | None = None) -> None:
        details = {"action": action, "resource": resource}
        if role:
            details["role"] = role
        super().__init__(
            f"Access denied: cannot '{action}' on '{resource}'",
            details,
        )


class PIIDetectedError(SecurityError):
    """PII detected in data."""

    def __init__(self, field: str, pii_type: str) -> None:
        super().__init__(
            f"PII detected in field '{field}'",
            {"field": field, "pii_type": pii_type},
        )


# Leader Election Errors
class LeaderElectionError(MongoClawError):
    """Error in leader election."""

    pass


class NotLeaderError(LeaderElectionError):
    """Current instance is not the leader."""

    def __init__(self, instance_id: str, leader_id: str) -> None:
        super().__init__(
            f"Instance '{instance_id}' is not the leader (leader: '{leader_id}')",
            {"instance_id": instance_id, "leader_id": leader_id},
        )
