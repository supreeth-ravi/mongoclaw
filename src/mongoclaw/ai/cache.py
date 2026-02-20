"""Response caching for AI completions."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any

from mongoclaw.core.types import AIResponse
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)


class ResponseCache:
    """
    In-memory cache for AI responses.

    Features:
    - LRU eviction
    - TTL-based expiration
    - Cache key generation from request parameters
    """

    def __init__(
        self,
        max_size: int = 1000,
        ttl_seconds: int = 3600,
    ) -> None:
        self._max_size = max_size
        self._ttl = timedelta(seconds=ttl_seconds)
        self._cache: dict[str, tuple[AIResponse, datetime]] = {}
        self._access_order: list[str] = []

        # Stats
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> AIResponse | None:
        """
        Get a cached response.

        Args:
            key: The cache key.

        Returns:
            Cached response or None.
        """
        if key not in self._cache:
            self._misses += 1
            return None

        response, cached_at = self._cache[key]

        # Check expiration
        if datetime.utcnow() - cached_at > self._ttl:
            self._remove(key)
            self._misses += 1
            return None

        # Update access order (LRU)
        self._touch(key)

        self._hits += 1
        logger.debug("Cache hit", key=key[:16])

        return response

    def set(self, key: str, response: AIResponse) -> None:
        """
        Cache a response.

        Args:
            key: The cache key.
            response: The response to cache.
        """
        # Evict if at capacity
        while len(self._cache) >= self._max_size:
            self._evict_oldest()

        self._cache[key] = (response, datetime.utcnow())
        self._access_order.append(key)

        logger.debug("Cached response", key=key[:16])

    def _touch(self, key: str) -> None:
        """Update access order for LRU."""
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    def _remove(self, key: str) -> None:
        """Remove a key from cache."""
        if key in self._cache:
            del self._cache[key]
        if key in self._access_order:
            self._access_order.remove(key)

    def _evict_oldest(self) -> None:
        """Evict the least recently used entry."""
        if self._access_order:
            oldest = self._access_order.pop(0)
            if oldest in self._cache:
                del self._cache[oldest]

    def clear(self) -> None:
        """Clear the cache."""
        self._cache.clear()
        self._access_order.clear()
        logger.info("Cache cleared")

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0.0

        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(hit_rate, 3),
            "ttl_seconds": self._ttl.total_seconds(),
        }

    @staticmethod
    def generate_key(
        model: str,
        prompt: str,
        system_prompt: str | None = None,
        temperature: float = 0.0,
        **kwargs: Any,
    ) -> str:
        """
        Generate a cache key from request parameters.

        Note: Only deterministic parameters should be used.
        Temperature > 0 makes responses non-deterministic.

        Args:
            model: The model identifier.
            prompt: The user prompt.
            system_prompt: Optional system prompt.
            temperature: Sampling temperature.
            **kwargs: Additional parameters.

        Returns:
            A hash key for caching.
        """
        # Only cache deterministic requests
        if temperature > 0:
            # Include timestamp to prevent caching
            kwargs["_ts"] = datetime.utcnow().isoformat()

        key_data = {
            "model": model,
            "prompt": prompt,
            "system_prompt": system_prompt,
            "temperature": temperature,
            **{k: v for k, v in kwargs.items() if k not in ("api_key",)},
        }

        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.sha256(key_str.encode()).hexdigest()


class RedisResponseCache:
    """
    Redis-backed cache for AI responses.

    For distributed caching across multiple instances.
    """

    def __init__(
        self,
        redis_client: Any,
        prefix: str = "mongoclaw:ai:cache:",
        ttl_seconds: int = 3600,
    ) -> None:
        self._redis = redis_client
        self._prefix = prefix
        self._ttl = ttl_seconds

    async def get(self, key: str) -> AIResponse | None:
        """Get a cached response from Redis."""
        try:
            data = await self._redis.get(f"{self._prefix}{key}")
            if data:
                parsed = json.loads(data)
                return AIResponse(**parsed)
            return None
        except Exception as e:
            logger.warning("Redis cache get failed", error=str(e))
            return None

    async def set(self, key: str, response: AIResponse) -> None:
        """Cache a response in Redis."""
        try:
            data = json.dumps(response.to_dict())
            await self._redis.setex(
                f"{self._prefix}{key}",
                self._ttl,
                data,
            )
        except Exception as e:
            logger.warning("Redis cache set failed", error=str(e))

    async def clear(self) -> None:
        """Clear all cached responses."""
        try:
            keys = await self._redis.keys(f"{self._prefix}*")
            if keys:
                await self._redis.delete(*keys)
        except Exception as e:
            logger.warning("Redis cache clear failed", error=str(e))
