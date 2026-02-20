"""Backpressure handling for worker pools."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)


class BackpressureController:
    """
    Controls processing rate based on system load.

    Features:
    - Queue depth monitoring
    - Adaptive rate limiting
    - Graceful degradation
    """

    def __init__(
        self,
        threshold: float = 0.8,
        check_interval: float = 5.0,
        min_rate: float = 0.1,
        max_rate: float = 1.0,
    ) -> None:
        """
        Initialize backpressure controller.

        Args:
            threshold: Queue fullness threshold (0-1) to trigger backpressure.
            check_interval: Seconds between checks.
            min_rate: Minimum rate multiplier.
            max_rate: Maximum rate multiplier (1.0 = full speed).
        """
        self._threshold = threshold
        self._check_interval = check_interval
        self._min_rate = min_rate
        self._max_rate = max_rate

        self._current_rate = max_rate
        self._backpressure_active = False
        self._last_check = 0.0

        # Metrics
        self._pressure_events = 0
        self._total_delay = 0.0

    @property
    def rate(self) -> float:
        """Get the current rate multiplier."""
        return self._current_rate

    @property
    def is_active(self) -> bool:
        """Check if backpressure is currently active."""
        return self._backpressure_active

    async def check(
        self,
        queue_size: int,
        queue_capacity: int,
    ) -> None:
        """
        Check and update backpressure state.

        Args:
            queue_size: Current queue size.
            queue_capacity: Maximum queue capacity.
        """
        now = time.time()

        if now - self._last_check < self._check_interval:
            return

        self._last_check = now

        if queue_capacity <= 0:
            return

        fullness = queue_size / queue_capacity

        if fullness >= self._threshold:
            # Activate or increase backpressure
            if not self._backpressure_active:
                self._backpressure_active = True
                self._pressure_events += 1
                logger.warning(
                    "Backpressure activated",
                    queue_fullness=round(fullness, 2),
                    threshold=self._threshold,
                )

            # Reduce rate proportionally
            pressure = (fullness - self._threshold) / (1 - self._threshold)
            self._current_rate = max(
                self._min_rate,
                self._max_rate * (1 - pressure),
            )

        else:
            # Gradually release backpressure
            if self._backpressure_active:
                self._current_rate = min(
                    self._max_rate,
                    self._current_rate * 1.1,  # 10% increase
                )

                if self._current_rate >= self._max_rate * 0.95:
                    self._backpressure_active = False
                    self._current_rate = self._max_rate
                    logger.info("Backpressure released")

    async def wait(self) -> None:
        """Wait based on current rate."""
        if self._current_rate >= self._max_rate:
            return

        # Calculate delay
        delay = (1 / self._current_rate) - 1
        if delay > 0:
            self._total_delay += delay
            await asyncio.sleep(delay)

    def get_stats(self) -> dict[str, Any]:
        """Get backpressure statistics."""
        return {
            "active": self._backpressure_active,
            "current_rate": round(self._current_rate, 3),
            "pressure_events": self._pressure_events,
            "total_delay_seconds": round(self._total_delay, 2),
        }


class SlidingWindowRateLimiter:
    """
    Sliding window rate limiter for work items.

    Limits the number of items processed per time window.
    """

    def __init__(
        self,
        max_rate: int,
        window_seconds: float = 60.0,
    ) -> None:
        """
        Initialize rate limiter.

        Args:
            max_rate: Maximum items per window.
            window_seconds: Window size in seconds.
        """
        self._max_rate = max_rate
        self._window = window_seconds

        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self, timeout: float | None = None) -> bool:
        """
        Acquire permission to process an item.

        Args:
            timeout: Maximum time to wait.

        Returns:
            True if permission granted.
        """
        start = time.time()

        while True:
            async with self._lock:
                now = time.time()
                cutoff = now - self._window

                # Remove old timestamps
                self._timestamps = [t for t in self._timestamps if t > cutoff]

                if len(self._timestamps) < self._max_rate:
                    self._timestamps.append(now)
                    return True

                # Calculate wait time
                oldest = self._timestamps[0]
                wait_time = oldest + self._window - now

            if timeout is not None:
                elapsed = time.time() - start
                if elapsed + wait_time > timeout:
                    return False

            await asyncio.sleep(min(wait_time, 0.1))

    @property
    def current_rate(self) -> int:
        """Get current rate (items in window)."""
        now = time.time()
        cutoff = now - self._window
        return len([t for t in self._timestamps if t > cutoff])

    def get_stats(self) -> dict[str, Any]:
        """Get rate limiter statistics."""
        return {
            "max_rate": self._max_rate,
            "window_seconds": self._window,
            "current_rate": self.current_rate,
            "utilization": round(self.current_rate / self._max_rate, 3),
        }


class LoadShedder:
    """
    Load shedding for extreme conditions.

    Drops requests when system is overloaded.
    """

    def __init__(
        self,
        shed_threshold: float = 0.95,
        recovery_threshold: float = 0.7,
    ) -> None:
        """
        Initialize load shedder.

        Args:
            shed_threshold: Load level to start shedding.
            recovery_threshold: Load level to stop shedding.
        """
        self._shed_threshold = shed_threshold
        self._recovery_threshold = recovery_threshold

        self._shedding = False
        self._shed_count = 0

    def should_shed(self, load: float) -> bool:
        """
        Check if a request should be shed.

        Args:
            load: Current load level (0-1).

        Returns:
            True if request should be dropped.
        """
        if self._shedding:
            if load < self._recovery_threshold:
                self._shedding = False
                logger.info("Load shedding stopped", load=round(load, 2))
                return False
            self._shed_count += 1
            return True

        if load >= self._shed_threshold:
            self._shedding = True
            self._shed_count += 1
            logger.warning(
                "Load shedding started",
                load=round(load, 2),
                threshold=self._shed_threshold,
            )
            return True

        return False

    def get_stats(self) -> dict[str, Any]:
        """Get load shedder statistics."""
        return {
            "shedding": self._shedding,
            "shed_count": self._shed_count,
            "shed_threshold": self._shed_threshold,
            "recovery_threshold": self._recovery_threshold,
        }
