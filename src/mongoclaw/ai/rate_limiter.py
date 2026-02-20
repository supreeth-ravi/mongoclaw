"""Rate limiting for AI providers."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Any

from mongoclaw.core.exceptions import AIRateLimitError
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)


class TokenBucket:
    """
    Token bucket rate limiter.

    Allows bursts up to bucket capacity while enforcing
    an average rate over time.
    """

    def __init__(
        self,
        rate: float,
        capacity: float | None = None,
    ) -> None:
        """
        Initialize token bucket.

        Args:
            rate: Tokens per second to add.
            capacity: Maximum tokens (defaults to rate).
        """
        self._rate = rate
        self._capacity = capacity or rate
        self._tokens = self._capacity
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0, timeout: float | None = None) -> bool:
        """
        Acquire tokens from the bucket.

        Args:
            tokens: Number of tokens to acquire.
            timeout: Maximum time to wait (None = don't wait).

        Returns:
            True if tokens acquired.
        """
        async with self._lock:
            self._refill()

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True

            if timeout is None:
                return False

            # Calculate wait time
            needed = tokens - self._tokens
            wait_time = needed / self._rate

            if wait_time > timeout:
                return False

            # Wait and refill
            await asyncio.sleep(wait_time)
            self._refill()
            self._tokens -= tokens
            return True

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_update
        self._tokens = min(
            self._capacity,
            self._tokens + elapsed * self._rate,
        )
        self._last_update = now

    @property
    def available(self) -> float:
        """Get available tokens."""
        self._refill()
        return self._tokens


class RateLimiter:
    """
    Rate limiter for AI providers.

    Supports:
    - Per-provider rate limits
    - Per-model rate limits
    - Request and token-based limits
    """

    def __init__(self) -> None:
        # Provider rate limits (requests per minute)
        self._provider_limits: dict[str, TokenBucket] = {}
        self._model_limits: dict[str, TokenBucket] = {}

        # Token rate limits (tokens per minute)
        self._provider_token_limits: dict[str, TokenBucket] = {}

        # Default limits by provider (requests per minute)
        self._default_limits: dict[str, float] = {
            "openai": 60,
            "anthropic": 60,
            "google": 60,
            "azure": 60,
            "cohere": 100,
            "groq": 30,
            "mistral": 60,
        }

    def set_limit(
        self,
        provider: str | None = None,
        model: str | None = None,
        requests_per_minute: float | None = None,
        tokens_per_minute: float | None = None,
    ) -> None:
        """
        Set rate limits.

        Args:
            provider: Provider name.
            model: Model name.
            requests_per_minute: Request rate limit.
            tokens_per_minute: Token rate limit.
        """
        if provider and requests_per_minute:
            self._provider_limits[provider] = TokenBucket(
                rate=requests_per_minute / 60,
                capacity=requests_per_minute / 10,  # Allow short bursts
            )

        if model and requests_per_minute:
            self._model_limits[model] = TokenBucket(
                rate=requests_per_minute / 60,
                capacity=requests_per_minute / 10,
            )

        if provider and tokens_per_minute:
            self._provider_token_limits[provider] = TokenBucket(
                rate=tokens_per_minute / 60,
                capacity=tokens_per_minute / 10,
            )

    async def acquire(
        self,
        provider: str,
        model: str | None = None,
        tokens: int = 1,
        timeout: float = 30.0,
    ) -> None:
        """
        Acquire permission for a request.

        Args:
            provider: Provider name.
            model: Optional model name.
            tokens: Estimated tokens for token-based limiting.
            timeout: Maximum wait time.

        Raises:
            AIRateLimitError: If rate limit exceeded.
        """
        # Check provider request limit
        if provider in self._provider_limits:
            bucket = self._provider_limits[provider]
            if not await bucket.acquire(1.0, timeout):
                raise AIRateLimitError(
                    provider=provider,
                    model=model,
                    retry_after=1.0 / bucket._rate,
                )

        # Check default provider limit
        elif provider in self._default_limits:
            # Create bucket on first use
            self._provider_limits[provider] = TokenBucket(
                rate=self._default_limits[provider] / 60,
                capacity=self._default_limits[provider] / 10,
            )
            bucket = self._provider_limits[provider]
            if not await bucket.acquire(1.0, timeout):
                raise AIRateLimitError(
                    provider=provider,
                    model=model,
                    retry_after=1.0 / bucket._rate,
                )

        # Check model limit
        if model and model in self._model_limits:
            bucket = self._model_limits[model]
            if not await bucket.acquire(1.0, timeout):
                raise AIRateLimitError(
                    provider=provider,
                    model=model,
                    retry_after=1.0 / bucket._rate,
                )

        # Check provider token limit
        if provider in self._provider_token_limits and tokens > 0:
            bucket = self._provider_token_limits[provider]
            if not await bucket.acquire(float(tokens), timeout):
                raise AIRateLimitError(
                    provider=provider,
                    model=model,
                    retry_after=float(tokens) / bucket._rate,
                )

    def get_stats(self) -> dict[str, Any]:
        """Get rate limiter statistics."""
        return {
            "provider_limits": {
                p: {
                    "available": round(b.available, 2),
                    "rate_per_min": round(b._rate * 60, 2),
                }
                for p, b in self._provider_limits.items()
            },
            "model_limits": {
                m: {
                    "available": round(b.available, 2),
                    "rate_per_min": round(b._rate * 60, 2),
                }
                for m, b in self._model_limits.items()
            },
        }


class AdaptiveRateLimiter(RateLimiter):
    """
    Rate limiter that adapts to provider responses.

    Automatically adjusts limits based on rate limit errors.
    """

    def __init__(self) -> None:
        super().__init__()
        self._error_counts: dict[str, int] = defaultdict(int)
        self._last_error_time: dict[str, float] = {}

    def record_error(
        self,
        provider: str,
        model: str | None = None,
        retry_after: float | None = None,
    ) -> None:
        """
        Record a rate limit error.

        Args:
            provider: Provider name.
            model: Optional model name.
            retry_after: Suggested retry time from provider.
        """
        key = f"{provider}:{model}" if model else provider
        self._error_counts[key] += 1
        self._last_error_time[key] = time.monotonic()

        # Reduce rate limit
        if provider in self._provider_limits:
            bucket = self._provider_limits[provider]
            # Reduce capacity by 20%
            bucket._capacity *= 0.8
            bucket._rate *= 0.8
            logger.warning(
                "Reduced rate limit due to errors",
                provider=provider,
                new_rate=round(bucket._rate * 60, 2),
            )

    def record_success(self, provider: str, model: str | None = None) -> None:
        """
        Record a successful request.

        Gradually restores rate limits after errors.
        """
        key = f"{provider}:{model}" if model else provider

        if key in self._error_counts and self._error_counts[key] > 0:
            # Decay error count
            self._error_counts[key] = max(0, self._error_counts[key] - 1)

            # Restore rate if no recent errors
            if (
                self._error_counts[key] == 0
                and provider in self._provider_limits
            ):
                bucket = self._provider_limits[provider]
                default_rate = self._default_limits.get(provider, 60) / 60
                if bucket._rate < default_rate:
                    # Restore by 10%
                    bucket._rate = min(default_rate, bucket._rate * 1.1)
                    bucket._capacity = min(
                        default_rate * 10,
                        bucket._capacity * 1.1,
                    )
