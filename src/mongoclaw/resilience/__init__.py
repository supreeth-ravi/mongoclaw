"""Resilience module for fault tolerance."""

from mongoclaw.resilience.circuit_breaker import CircuitBreaker, CircuitBreakerRegistry
from mongoclaw.resilience.retry import RetryPolicy, retry_with_policy
from mongoclaw.resilience.health import HealthChecker, HealthStatus
from mongoclaw.resilience.shutdown import GracefulShutdown

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerRegistry",
    "RetryPolicy",
    "retry_with_policy",
    "HealthChecker",
    "HealthStatus",
    "GracefulShutdown",
]
