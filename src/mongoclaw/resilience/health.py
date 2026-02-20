"""Health check system for monitoring component status."""

from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import Any, Callable, Coroutine

from mongoclaw.core.types import HealthStatus
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)


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
            "latency_ms": round(self.latency_ms, 2),
            "details": self.details,
        }


HealthCheckFunc = Callable[[], Coroutine[Any, Any, HealthCheckResult]]


class HealthChecker:
    """
    Coordinates health checks across multiple components.

    Features:
    - Async health checks with timeouts
    - Aggregated health status
    - Caching to prevent check storms
    """

    def __init__(
        self,
        timeout: float = 10.0,
        cache_ttl: float = 5.0,
    ) -> None:
        """
        Initialize health checker.

        Args:
            timeout: Timeout for individual checks.
            cache_ttl: Cache TTL for results.
        """
        self._timeout = timeout
        self._cache_ttl = cache_ttl

        self._checks: dict[str, HealthCheckFunc] = {}
        self._cache: dict[str, tuple[HealthCheckResult, float]] = {}
        self._lock = asyncio.Lock()

    def register(
        self,
        component: str,
        check_func: HealthCheckFunc,
    ) -> None:
        """
        Register a health check function.

        Args:
            component: Component name.
            check_func: Async function returning HealthCheckResult.
        """
        self._checks[component] = check_func
        logger.debug("Registered health check", component=component)

    def unregister(self, component: str) -> None:
        """Unregister a health check."""
        self._checks.pop(component, None)
        self._cache.pop(component, None)

    async def check(self, component: str) -> HealthCheckResult:
        """
        Run a single health check.

        Args:
            component: Component to check.

        Returns:
            HealthCheckResult.
        """
        # Check cache
        cached = self._get_cached(component)
        if cached:
            return cached

        if component not in self._checks:
            return HealthCheckResult(
                component=component,
                status=HealthStatus.UNHEALTHY,
                message=f"Unknown component: {component}",
            )

        check_func = self._checks[component]
        start_time = time.perf_counter()

        try:
            result = await asyncio.wait_for(
                check_func(),
                timeout=self._timeout,
            )
            result.latency_ms = (time.perf_counter() - start_time) * 1000

        except asyncio.TimeoutError:
            result = HealthCheckResult(
                component=component,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check timed out after {self._timeout}s",
                latency_ms=(time.perf_counter() - start_time) * 1000,
            )

        except Exception as e:
            result = HealthCheckResult(
                component=component,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check failed: {e}",
                latency_ms=(time.perf_counter() - start_time) * 1000,
            )

        # Cache result
        self._set_cached(component, result)

        return result

    async def check_all(self) -> dict[str, HealthCheckResult]:
        """
        Run all registered health checks.

        Returns:
            Dictionary of component -> HealthCheckResult.
        """
        results: dict[str, HealthCheckResult] = {}

        # Run checks concurrently
        tasks = {
            component: asyncio.create_task(self.check(component))
            for component in self._checks
        }

        for component, task in tasks.items():
            results[component] = await task

        return results

    async def get_aggregate_status(self) -> tuple[HealthStatus, dict[str, Any]]:
        """
        Get aggregate health status across all components.

        Returns:
            Tuple of (overall status, details dict).
        """
        results = await self.check_all()

        statuses = [r.status for r in results.values()]

        if all(s == HealthStatus.HEALTHY for s in statuses):
            overall = HealthStatus.HEALTHY
        elif any(s == HealthStatus.UNHEALTHY for s in statuses):
            overall = HealthStatus.UNHEALTHY
        else:
            overall = HealthStatus.DEGRADED

        details = {
            "status": overall.value,
            "components": {
                name: result.to_dict()
                for name, result in results.items()
            },
            "healthy_count": sum(1 for s in statuses if s == HealthStatus.HEALTHY),
            "total_count": len(statuses),
        }

        return overall, details

    def _get_cached(self, component: str) -> HealthCheckResult | None:
        """Get cached result if still valid."""
        if component in self._cache:
            result, timestamp = self._cache[component]
            if time.time() - timestamp < self._cache_ttl:
                return result
        return None

    def _set_cached(self, component: str, result: HealthCheckResult) -> None:
        """Cache a result."""
        self._cache[component] = (result, time.time())

    def clear_cache(self) -> None:
        """Clear the result cache."""
        self._cache.clear()


# Predefined health check factories


async def mongodb_health_check(
    client: Any,
    timeout: float = 5.0,
) -> HealthCheckResult:
    """Create a MongoDB health check."""
    try:
        await asyncio.wait_for(
            client.admin.command("ping"),
            timeout=timeout,
        )
        return HealthCheckResult(
            component="mongodb",
            status=HealthStatus.HEALTHY,
            message="MongoDB is responsive",
        )
    except Exception as e:
        return HealthCheckResult(
            component="mongodb",
            status=HealthStatus.UNHEALTHY,
            message=f"MongoDB check failed: {e}",
        )


async def redis_health_check(
    client: Any,
    timeout: float = 5.0,
) -> HealthCheckResult:
    """Create a Redis health check."""
    try:
        await asyncio.wait_for(
            client.ping(),
            timeout=timeout,
        )
        return HealthCheckResult(
            component="redis",
            status=HealthStatus.HEALTHY,
            message="Redis is responsive",
        )
    except Exception as e:
        return HealthCheckResult(
            component="redis",
            status=HealthStatus.UNHEALTHY,
            message=f"Redis check failed: {e}",
        )


def create_health_check_factory(
    component: str,
    check_func: Callable[[], Coroutine[Any, Any, bool]],
    healthy_message: str = "OK",
    unhealthy_message: str = "Check failed",
) -> HealthCheckFunc:
    """
    Create a health check function from a simple boolean check.

    Args:
        component: Component name.
        check_func: Async function returning True if healthy.
        healthy_message: Message when healthy.
        unhealthy_message: Message when unhealthy.

    Returns:
        HealthCheckFunc.
    """

    async def health_check() -> HealthCheckResult:
        try:
            is_healthy = await check_func()
            return HealthCheckResult(
                component=component,
                status=HealthStatus.HEALTHY if is_healthy else HealthStatus.UNHEALTHY,
                message=healthy_message if is_healthy else unhealthy_message,
            )
        except Exception as e:
            return HealthCheckResult(
                component=component,
                status=HealthStatus.UNHEALTHY,
                message=str(e),
            )

    return health_check
