"""Worker pool for managing concurrent work item processing."""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Any

from mongoclaw.core.config import Settings
from mongoclaw.dispatcher.routing import RoutingStrategy, get_all_stream_patterns
from mongoclaw.observability.logging import get_logger
from mongoclaw.worker.agent_worker import AgentWorker

if TYPE_CHECKING:
    from mongoclaw.agents.store import AgentStore
    from mongoclaw.queue.redis_stream import RedisStreamBackend

logger = get_logger(__name__)


class WorkerPool:
    """
    Manages a pool of workers for processing work items.

    Features:
    - Dynamic pool sizing
    - Stream discovery and subscription
    - Graceful shutdown with drain
    - Health monitoring
    """

    def __init__(
        self,
        queue_backend: RedisStreamBackend,
        agent_store: AgentStore,
        settings: Settings,
        pool_size: int | None = None,
        routing_strategy: RoutingStrategy = RoutingStrategy.BY_AGENT,
    ) -> None:
        self._queue = queue_backend
        self._agent_store = agent_store
        self._settings = settings
        self._pool_size = pool_size or settings.worker.pool_size
        self._routing_strategy = routing_strategy

        self._pool_id = f"pool-{uuid.uuid4().hex[:8]}"
        self._workers: list[AgentWorker] = []
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Stream tracking
        self._active_streams: set[str] = set()

        # Stats
        self._processed_count = 0
        self._error_count = 0

    @property
    def pool_id(self) -> str:
        """Get the pool identifier."""
        return self._pool_id

    @property
    def is_running(self) -> bool:
        """Check if pool is running."""
        return self._running

    @property
    def worker_count(self) -> int:
        """Get the current number of workers."""
        return len(self._workers)

    async def start(self) -> None:
        """Start the worker pool."""
        if self._running:
            logger.warning("Worker pool already running")
            return

        logger.info(
            "Starting worker pool",
            pool_id=self._pool_id,
            pool_size=self._pool_size,
        )

        self._running = True
        self._shutdown_event.clear()

        # Discover streams to process
        await self._discover_streams()

        # Create and start workers
        for i in range(self._pool_size):
            worker = AgentWorker(
                worker_id=f"{self._pool_id}-worker-{i}",
                queue_backend=self._queue,
                agent_store=self._agent_store,
                settings=self._settings,
                streams=list(self._active_streams),
            )
            self._workers.append(worker)

            task = asyncio.create_task(
                worker.run(self._shutdown_event),
                name=f"worker_{i}",
            )
            self._worker_tasks.append(task)

        # Start stream discovery loop
        asyncio.create_task(self._stream_discovery_loop())

        logger.info(
            "Worker pool started",
            pool_id=self._pool_id,
            workers=len(self._workers),
            streams=len(self._active_streams),
        )

    async def shutdown(self, timeout: float | None = None) -> None:
        """
        Gracefully shutdown the worker pool.

        Args:
            timeout: Maximum time to wait for workers to finish.
        """
        if not self._running:
            return

        timeout = timeout or self._settings.worker.shutdown_timeout

        logger.info(
            "Shutting down worker pool",
            pool_id=self._pool_id,
            timeout=timeout,
        )

        # Signal workers to stop
        self._running = False
        self._shutdown_event.set()

        # Wait for workers to finish
        if self._worker_tasks:
            done, pending = await asyncio.wait(
                self._worker_tasks,
                timeout=timeout,
            )

            # Cancel any remaining tasks
            for task in pending:
                task.cancel()

            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        # Collect stats from workers
        for worker in self._workers:
            stats = worker.get_stats()
            self._processed_count += stats.get("processed_count", 0)
            self._error_count += stats.get("error_count", 0)

        self._workers.clear()
        self._worker_tasks.clear()

        logger.info(
            "Worker pool shutdown complete",
            pool_id=self._pool_id,
            total_processed=self._processed_count,
            total_errors=self._error_count,
        )

    async def _discover_streams(self) -> None:
        """Discover streams to process."""
        patterns = get_all_stream_patterns(self._routing_strategy)

        streams: set[str] = set()

        for pattern in patterns:
            if "*" in pattern:
                # Scan for matching streams
                discovered = await self._scan_streams(pattern)
                streams.update(discovered)
            else:
                streams.add(pattern)

        # Also get agent-specific streams
        agents = await self._agent_store.list(enabled_only=True)
        for agent in agents:
            stream = f"mongoclaw:agent:{agent.id}"
            streams.add(stream)

        self._active_streams = streams

        logger.debug(
            "Discovered streams",
            count=len(streams),
            streams=list(streams)[:10],  # Log first 10
        )

    async def _scan_streams(self, pattern: str) -> set[str]:
        """Scan Redis for streams matching a pattern."""
        try:
            keys = await self._queue.client.keys(pattern)
            return {k for k in keys if await self._is_stream(k)}
        except Exception:
            return set()

    async def _is_stream(self, key: str) -> bool:
        """Check if a key is a Redis stream."""
        try:
            key_type = await self._queue.client.type(key)
            return key_type == "stream"
        except Exception:
            return False

    async def _stream_discovery_loop(self) -> None:
        """Periodically discover new streams."""
        while self._running:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds

                old_streams = self._active_streams.copy()
                await self._discover_streams()

                new_streams = self._active_streams - old_streams
                if new_streams:
                    logger.info(
                        "Discovered new streams",
                        count=len(new_streams),
                        streams=list(new_streams),
                    )

                    # Update workers with new streams
                    for worker in self._workers:
                        worker.update_streams(list(self._active_streams))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Stream discovery error", error=str(e))

    async def scale(self, target_size: int) -> None:
        """
        Scale the worker pool to a target size.

        Args:
            target_size: Target number of workers.
        """
        current_size = len(self._workers)

        if target_size == current_size:
            return

        logger.info(
            "Scaling worker pool",
            from_size=current_size,
            to_size=target_size,
        )

        if target_size > current_size:
            # Scale up
            for i in range(current_size, target_size):
                worker = AgentWorker(
                    worker_id=f"{self._pool_id}-worker-{i}",
                    queue_backend=self._queue,
                    agent_store=self._agent_store,
                    settings=self._settings,
                    streams=list(self._active_streams),
                )
                self._workers.append(worker)

                task = asyncio.create_task(
                    worker.run(self._shutdown_event),
                    name=f"worker_{i}",
                )
                self._worker_tasks.append(task)

        else:
            # Scale down
            workers_to_stop = self._workers[target_size:]
            self._workers = self._workers[:target_size]

            for worker in workers_to_stop:
                await worker.stop()

            # Remove completed tasks
            self._worker_tasks = [
                t for t in self._worker_tasks
                if not t.done()
            ]

        self._pool_size = target_size

    def get_stats(self) -> dict[str, Any]:
        """Get pool statistics."""
        worker_stats = [w.get_stats() for w in self._workers]

        total_processed = sum(s.get("processed_count", 0) for s in worker_stats)
        total_errors = sum(s.get("error_count", 0) for s in worker_stats)

        return {
            "pool_id": self._pool_id,
            "running": self._running,
            "pool_size": self._pool_size,
            "active_workers": len([w for w in self._workers if w.is_running]),
            "active_streams": len(self._active_streams),
            "total_processed": self._processed_count + total_processed,
            "total_errors": self._error_count + total_errors,
            "workers": worker_stats,
        }

    async def health_check(self) -> dict[str, Any]:
        """Check pool health."""
        active_count = sum(1 for w in self._workers if w.is_running)

        return {
            "healthy": active_count > 0,
            "pool_id": self._pool_id,
            "active_workers": active_count,
            "total_workers": len(self._workers),
        }
