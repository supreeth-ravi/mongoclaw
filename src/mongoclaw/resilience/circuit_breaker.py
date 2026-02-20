"""Circuit breaker implementation for fault isolation."""

from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import Any, Callable, TypeVar

from mongoclaw.core.exceptions import CircuitBreakerOpenError
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if recovered


class CircuitBreaker:
    """
    Circuit breaker for protecting against cascading failures.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Too many failures, requests are rejected
    - HALF_OPEN: Testing recovery, limited requests allowed
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout: float = 30.0,
        half_open_max_calls: int = 3,
    ) -> None:
        """
        Initialize circuit breaker.

        Args:
            name: Circuit breaker name.
            failure_threshold: Failures before opening.
            success_threshold: Successes in half-open to close.
            timeout: Seconds before transitioning open -> half-open.
            half_open_max_calls: Max concurrent calls in half-open state.
        """
        self._name = name
        self._failure_threshold = failure_threshold
        self._success_threshold = success_threshold
        self._timeout = timeout
        self._half_open_max_calls = half_open_max_calls

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float | None = None
        self._half_open_calls = 0

        self._lock = asyncio.Lock()

    @property
    def name(self) -> str:
        """Get circuit breaker name."""
        return self._name

    @property
    def state(self) -> CircuitState:
        """Get current state."""
        return self._state

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed."""
        return self._state == CircuitState.CLOSED

    async def call(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Execute a function with circuit breaker protection.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Function result.

        Raises:
            CircuitBreakerOpenError: If circuit is open.
        """
        await self._check_state()

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            await self._record_success()
            return result

        except Exception as e:
            await self._record_failure()
            raise

    async def _check_state(self) -> None:
        """Check and potentially transition state."""
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return

            if self._state == CircuitState.OPEN:
                # Check if timeout has passed
                if self._last_failure_time:
                    elapsed = time.time() - self._last_failure_time
                    if elapsed >= self._timeout:
                        self._transition_to_half_open()
                        return

                raise CircuitBreakerOpenError(
                    self._name,
                    self._failure_count,
                )

            if self._state == CircuitState.HALF_OPEN:
                # Allow limited calls
                if self._half_open_calls >= self._half_open_max_calls:
                    raise CircuitBreakerOpenError(
                        self._name,
                        self._failure_count,
                    )
                self._half_open_calls += 1

    async def _record_success(self) -> None:
        """Record a successful call."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                self._half_open_calls = max(0, self._half_open_calls - 1)

                if self._success_count >= self._success_threshold:
                    self._transition_to_closed()

            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0

    async def _record_failure(self) -> None:
        """Record a failed call."""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                # Any failure in half-open goes back to open
                self._transition_to_open()

            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self._failure_threshold:
                    self._transition_to_open()

    def _transition_to_open(self) -> None:
        """Transition to open state."""
        logger.warning(
            "Circuit breaker opened",
            name=self._name,
            failures=self._failure_count,
        )
        self._state = CircuitState.OPEN
        self._success_count = 0
        self._half_open_calls = 0

    def _transition_to_half_open(self) -> None:
        """Transition to half-open state."""
        logger.info(
            "Circuit breaker half-open",
            name=self._name,
        )
        self._state = CircuitState.HALF_OPEN
        self._success_count = 0
        self._half_open_calls = 0

    def _transition_to_closed(self) -> None:
        """Transition to closed state."""
        logger.info(
            "Circuit breaker closed",
            name=self._name,
        )
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0

    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._half_open_calls = 0

    def get_stats(self) -> dict[str, Any]:
        """Get circuit breaker statistics."""
        return {
            "name": self._name,
            "state": self._state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "failure_threshold": self._failure_threshold,
            "success_threshold": self._success_threshold,
            "timeout_seconds": self._timeout,
        }


class CircuitBreakerRegistry:
    """
    Registry for managing multiple circuit breakers.

    Typically one breaker per external service/provider.
    """

    def __init__(self) -> None:
        self._breakers: dict[str, CircuitBreaker] = {}

    def get(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout: float = 30.0,
    ) -> CircuitBreaker:
        """
        Get or create a circuit breaker.

        Args:
            name: Circuit breaker name.
            failure_threshold: Failures before opening.
            success_threshold: Successes to close.
            timeout: Open state timeout.

        Returns:
            CircuitBreaker instance.
        """
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(
                name=name,
                failure_threshold=failure_threshold,
                success_threshold=success_threshold,
                timeout=timeout,
            )

        return self._breakers[name]

    def get_all(self) -> dict[str, CircuitBreaker]:
        """Get all circuit breakers."""
        return self._breakers.copy()

    def get_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all breakers."""
        return {
            name: breaker.get_stats()
            for name, breaker in self._breakers.items()
        }

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        for breaker in self._breakers.values():
            breaker.reset()


# Global registry
_registry: CircuitBreakerRegistry | None = None


def get_circuit_breaker_registry() -> CircuitBreakerRegistry:
    """Get the global circuit breaker registry."""
    global _registry
    if _registry is None:
        _registry = CircuitBreakerRegistry()
    return _registry
