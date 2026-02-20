"""Graceful shutdown handling."""

from __future__ import annotations

import asyncio
import signal
from typing import Any, Callable, Coroutine

from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)

ShutdownHandler = Callable[[], Coroutine[Any, Any, None]]


class GracefulShutdown:
    """
    Manages graceful shutdown of application components.

    Features:
    - Signal handling (SIGINT, SIGTERM)
    - Ordered shutdown with dependencies
    - Timeout handling
    - Drain mode for workers
    """

    def __init__(
        self,
        timeout: float = 30.0,
        drain_timeout: float = 10.0,
    ) -> None:
        """
        Initialize shutdown manager.

        Args:
            timeout: Maximum time for shutdown.
            drain_timeout: Time to wait for in-flight work.
        """
        self._timeout = timeout
        self._drain_timeout = drain_timeout

        self._handlers: list[tuple[str, ShutdownHandler, int]] = []
        self._shutdown_event = asyncio.Event()
        self._shutting_down = False
        self._signals_registered = False

    @property
    def is_shutting_down(self) -> bool:
        """Check if shutdown is in progress."""
        return self._shutting_down

    def register(
        self,
        name: str,
        handler: ShutdownHandler,
        priority: int = 0,
    ) -> None:
        """
        Register a shutdown handler.

        Args:
            name: Handler name for logging.
            handler: Async function to call on shutdown.
            priority: Higher priority runs first (default 0).
        """
        self._handlers.append((name, handler, priority))
        logger.debug("Registered shutdown handler", name=name, priority=priority)

    def register_signals(self) -> None:
        """Register signal handlers for SIGINT and SIGTERM."""
        if self._signals_registered:
            return

        loop = asyncio.get_running_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(self._handle_signal(s)),
            )

        self._signals_registered = True
        logger.debug("Signal handlers registered")

    async def _handle_signal(self, sig: signal.Signals) -> None:
        """Handle shutdown signal."""
        logger.info("Received shutdown signal", signal=sig.name)
        await self.shutdown()

    async def shutdown(self) -> None:
        """
        Execute graceful shutdown.

        Runs all handlers in priority order with timeout.
        """
        if self._shutting_down:
            logger.warning("Shutdown already in progress")
            return

        self._shutting_down = True
        self._shutdown_event.set()

        logger.info(
            "Starting graceful shutdown",
            timeout=self._timeout,
            handler_count=len(self._handlers),
        )

        # Sort handlers by priority (higher first)
        sorted_handlers = sorted(
            self._handlers,
            key=lambda x: x[2],
            reverse=True,
        )

        # Wait for drain period first
        logger.info("Draining in-flight work", timeout=self._drain_timeout)
        await asyncio.sleep(min(self._drain_timeout, 2.0))

        # Execute handlers with overall timeout
        try:
            await asyncio.wait_for(
                self._run_handlers(sorted_handlers),
                timeout=self._timeout,
            )
            logger.info("Graceful shutdown completed")

        except asyncio.TimeoutError:
            logger.warning(
                "Shutdown timed out, forcing exit",
                timeout=self._timeout,
            )

        except Exception as e:
            logger.error("Shutdown error", error=str(e))

    async def _run_handlers(
        self,
        handlers: list[tuple[str, ShutdownHandler, int]],
    ) -> None:
        """Run shutdown handlers in order."""
        for name, handler, priority in handlers:
            try:
                logger.debug("Running shutdown handler", name=name)
                await handler()
                logger.debug("Shutdown handler completed", name=name)

            except Exception as e:
                logger.error(
                    "Shutdown handler failed",
                    name=name,
                    error=str(e),
                )

    async def wait_for_shutdown(self) -> None:
        """Wait until shutdown signal is received."""
        await self._shutdown_event.wait()

    def create_shutdown_context(self) -> ShutdownContext:
        """Create a context manager for components."""
        return ShutdownContext(self)


class ShutdownContext:
    """Context manager for component lifecycle."""

    def __init__(self, shutdown_manager: GracefulShutdown) -> None:
        self._manager = shutdown_manager

    async def __aenter__(self) -> ShutdownContext:
        self._manager.register_signals()
        return self

    async def __aexit__(self, *args: Any) -> None:
        if not self._manager.is_shutting_down:
            await self._manager.shutdown()


class DrainableWorker:
    """
    Mixin for workers that support draining.

    Provides a standard interface for graceful work completion.
    """

    def __init__(self) -> None:
        self._draining = False
        self._active_work_count = 0
        self._drain_complete = asyncio.Event()

    @property
    def is_draining(self) -> bool:
        """Check if worker is draining."""
        return self._draining

    def start_drain(self) -> None:
        """Start draining (stop accepting new work)."""
        self._draining = True
        logger.info("Worker draining started")

        if self._active_work_count == 0:
            self._drain_complete.set()

    def work_started(self) -> None:
        """Called when work item processing starts."""
        self._active_work_count += 1

    def work_completed(self) -> None:
        """Called when work item processing completes."""
        self._active_work_count -= 1

        if self._draining and self._active_work_count == 0:
            self._drain_complete.set()
            logger.info("Worker drain complete")

    async def wait_for_drain(self, timeout: float | None = None) -> bool:
        """
        Wait for all active work to complete.

        Args:
            timeout: Maximum time to wait.

        Returns:
            True if drain completed, False if timed out.
        """
        try:
            await asyncio.wait_for(
                self._drain_complete.wait(),
                timeout=timeout,
            )
            return True
        except asyncio.TimeoutError:
            return False

    def get_drain_stats(self) -> dict[str, Any]:
        """Get drain statistics."""
        return {
            "draining": self._draining,
            "active_work_count": self._active_work_count,
            "drain_complete": self._drain_complete.is_set(),
        }
