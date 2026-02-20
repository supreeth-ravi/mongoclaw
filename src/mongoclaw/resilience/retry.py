"""Retry logic with exponential backoff."""

from __future__ import annotations

import asyncio
import random
from functools import wraps
from typing import Any, Callable, Sequence, TypeVar

from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random_exponential,
)

from mongoclaw.core.exceptions import AIRateLimitError, MongoClawError
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class RetryPolicy:
    """
    Configurable retry policy.

    Features:
    - Exponential backoff with jitter
    - Configurable exception filtering
    - Max attempts and delays
    """

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: Sequence[type[Exception]] | None = None,
        non_retryable_exceptions: Sequence[type[Exception]] | None = None,
    ) -> None:
        """
        Initialize retry policy.

        Args:
            max_attempts: Maximum retry attempts.
            base_delay: Base delay in seconds.
            max_delay: Maximum delay in seconds.
            exponential_base: Base for exponential backoff.
            jitter: Add random jitter to delays.
            retryable_exceptions: Exceptions that trigger retry.
            non_retryable_exceptions: Exceptions that should not retry.
        """
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter

        self.retryable_exceptions = retryable_exceptions or (
            ConnectionError,
            TimeoutError,
            AIRateLimitError,
        )
        self.non_retryable_exceptions = non_retryable_exceptions or ()

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for a given attempt number.

        Args:
            attempt: Current attempt (1-based).

        Returns:
            Delay in seconds.
        """
        delay = self.base_delay * (self.exponential_base ** (attempt - 1))
        delay = min(delay, self.max_delay)

        if self.jitter:
            # Add random jitter (0-50% of delay)
            jitter_amount = delay * random.uniform(0, 0.5)
            delay += jitter_amount

        return delay

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """
        Determine if an exception should trigger a retry.

        Args:
            exception: The exception that occurred.
            attempt: Current attempt number.

        Returns:
            True if should retry.
        """
        if attempt >= self.max_attempts:
            return False

        if self.non_retryable_exceptions:
            if isinstance(exception, self.non_retryable_exceptions):
                return False

        if self.retryable_exceptions:
            return isinstance(exception, self.retryable_exceptions)

        return True


async def retry_with_policy(
    func: Callable[..., Any],
    policy: RetryPolicy,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    Execute a function with retry policy.

    Args:
        func: Async function to execute.
        policy: Retry policy to use.
        *args: Function arguments.
        **kwargs: Function keyword arguments.

    Returns:
        Function result.

    Raises:
        The last exception if all retries fail.
    """
    last_exception: Exception | None = None

    for attempt in range(1, policy.max_attempts + 1):
        try:
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            else:
                return func(*args, **kwargs)

        except Exception as e:
            last_exception = e

            if not policy.should_retry(e, attempt):
                raise

            delay = policy.calculate_delay(attempt)

            logger.warning(
                "Retrying after error",
                attempt=attempt,
                max_attempts=policy.max_attempts,
                error=str(e),
                delay=round(delay, 2),
            )

            await asyncio.sleep(delay)

    if last_exception:
        raise last_exception


def with_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: Sequence[type[Exception]] | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for adding retry behavior to functions.

    Args:
        max_attempts: Maximum retry attempts.
        base_delay: Base delay in seconds.
        max_delay: Maximum delay in seconds.
        retryable_exceptions: Exceptions that trigger retry.

    Returns:
        Decorator function.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            policy = RetryPolicy(
                max_attempts=max_attempts,
                base_delay=base_delay,
                max_delay=max_delay,
                retryable_exceptions=retryable_exceptions,
            )
            return await retry_with_policy(func, policy, *args, **kwargs)

        return wrapper  # type: ignore

    return decorator


def create_tenacity_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: tuple[type[Exception], ...] | None = None,
) -> AsyncRetrying:
    """
    Create a tenacity retry configuration.

    Args:
        max_attempts: Maximum retry attempts.
        base_delay: Base delay in seconds.
        max_delay: Maximum delay in seconds.
        retryable_exceptions: Exceptions that trigger retry.

    Returns:
        AsyncRetrying instance for use with async for.
    """
    return AsyncRetrying(
        stop=stop_after_attempt(max_attempts),
        wait=wait_random_exponential(min=base_delay, max=max_delay),
        retry=retry_if_exception_type(
            retryable_exceptions or (ConnectionError, TimeoutError)
        ),
        reraise=True,
    )


class RetryBudget:
    """
    Retry budget to prevent retry storms.

    Limits the total number of retries across all requests
    within a time window.
    """

    def __init__(
        self,
        max_retries_per_second: float = 10.0,
        budget_ratio: float = 0.2,
    ) -> None:
        """
        Initialize retry budget.

        Args:
            max_retries_per_second: Maximum retries per second.
            budget_ratio: Ratio of requests that can be retries.
        """
        self._max_retries_per_second = max_retries_per_second
        self._budget_ratio = budget_ratio

        self._request_count = 0
        self._retry_count = 0
        self._window_start = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()

    async def can_retry(self) -> bool:
        """
        Check if a retry is allowed.

        Returns:
            True if retry is within budget.
        """
        async with self._lock:
            now = asyncio.get_event_loop().time()

            # Reset window every second
            if now - self._window_start >= 1.0:
                self._request_count = 0
                self._retry_count = 0
                self._window_start = now

            # Check rate limit
            if self._retry_count >= self._max_retries_per_second:
                return False

            # Check budget ratio
            if self._request_count > 0:
                ratio = self._retry_count / self._request_count
                if ratio >= self._budget_ratio:
                    return False

            return True

    async def record_request(self) -> None:
        """Record a new request."""
        async with self._lock:
            self._request_count += 1

    async def record_retry(self) -> None:
        """Record a retry attempt."""
        async with self._lock:
            self._retry_count += 1

    def get_stats(self) -> dict[str, Any]:
        """Get budget statistics."""
        return {
            "request_count": self._request_count,
            "retry_count": self._retry_count,
            "retry_ratio": (
                self._retry_count / self._request_count
                if self._request_count > 0
                else 0.0
            ),
            "budget_ratio": self._budget_ratio,
        }
