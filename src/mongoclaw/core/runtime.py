"""Main runtime orchestrator for MongoClaw."""

from __future__ import annotations

import asyncio
import signal
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from motor.motor_asyncio import AsyncIOMotorClient

from mongoclaw.agents.store import AgentStore
from mongoclaw.core.config import Settings, get_settings
from mongoclaw.core.exceptions import MongoClawError
from mongoclaw.observability.logging import configure_logging, get_logger

logger = get_logger(__name__)


class Runtime:
    """
    Main runtime orchestrator for MongoClaw.

    Manages the lifecycle of all components:
    - MongoDB connections
    - Redis connections
    - Change stream watchers
    - Worker pools
    - API server
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Component instances (initialized on start)
        self._mongo_client: AsyncIOMotorClient[dict[str, Any]] | None = None
        self._agent_store: AgentStore | None = None
        self._watcher: Any | None = None  # ChangeStreamWatcher
        self._dispatcher: Any | None = None  # AgentDispatcher
        self._worker_pool: Any | None = None  # WorkerPool
        self._queue_backend: Any | None = None  # QueueBackend

        # Background tasks
        self._tasks: list[asyncio.Task[Any]] = []

    @property
    def settings(self) -> Settings:
        """Get the runtime settings."""
        return self._settings

    @property
    def is_running(self) -> bool:
        """Check if runtime is running."""
        return self._running

    @property
    def mongo_client(self) -> AsyncIOMotorClient[dict[str, Any]]:
        """Get the MongoDB client."""
        if self._mongo_client is None:
            raise RuntimeError("Runtime not started")
        return self._mongo_client

    @property
    def agent_store(self) -> AgentStore:
        """Get the agent store."""
        if self._agent_store is None:
            raise RuntimeError("Runtime not started")
        return self._agent_store

    async def start(self) -> None:
        """
        Start the runtime.

        Initializes all components and starts background tasks.
        """
        if self._running:
            logger.warning("Runtime already running")
            return

        logger.info(
            "Starting MongoClaw runtime",
            environment=self._settings.environment,
        )

        try:
            # Configure logging
            configure_logging(
                level=self._settings.observability.log_level,
                format_type=self._settings.observability.log_format,
            )

            # Initialize MongoDB connection
            await self._init_mongodb()

            # Initialize agent store
            self._agent_store = AgentStore(
                client=self._mongo_client,
                database=self._settings.mongodb.database,
                collection=self._settings.mongodb.agents_collection,
            )
            await self._agent_store.initialize()

            # Initialize queue backend (Redis)
            await self._init_queue()

            # Initialize components
            await self._init_watcher()
            await self._init_dispatcher()
            await self._init_worker_pool()

            # Start background tasks
            await self._start_watcher()
            await self._start_worker_pool()

            # Register signal handlers
            self._register_signal_handlers()

            self._running = True
            logger.info("MongoClaw runtime started successfully")

        except Exception as e:
            logger.error("Failed to start runtime", error=str(e))
            await self.stop()
            raise

    async def stop(self) -> None:
        """
        Stop the runtime gracefully.

        Drains work and closes all connections.
        """
        if not self._running:
            return

        logger.info("Stopping MongoClaw runtime")
        self._shutdown_event.set()

        # Cancel background tasks
        for task in self._tasks:
            task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()

        # Stop components in reverse order
        if self._worker_pool:
            await self._stop_worker_pool()

        if self._watcher:
            await self._stop_watcher()

        if self._queue_backend:
            await self._stop_queue()

        # Close MongoDB connection
        if self._mongo_client:
            self._mongo_client.close()
            self._mongo_client = None

        self._running = False
        logger.info("MongoClaw runtime stopped")

    async def run_forever(self) -> None:
        """
        Run the runtime until shutdown signal.
        """
        await self.start()

        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    @asynccontextmanager
    async def context(self) -> AsyncIterator[Runtime]:
        """
        Context manager for runtime lifecycle.

        Usage:
            async with runtime.context() as rt:
                # Runtime is running
                pass
            # Runtime is stopped
        """
        await self.start()
        try:
            yield self
        finally:
            await self.stop()

    async def _init_mongodb(self) -> None:
        """Initialize MongoDB connection."""
        logger.debug("Connecting to MongoDB")

        self._mongo_client = AsyncIOMotorClient(
            self._settings.mongodb.uri.get_secret_value(),
            maxPoolSize=self._settings.mongodb.max_pool_size,
            minPoolSize=self._settings.mongodb.min_pool_size,
            serverSelectionTimeoutMS=self._settings.mongodb.server_selection_timeout_ms,
        )

        # Verify connection
        await self._mongo_client.admin.command("ping")
        logger.info("Connected to MongoDB")

    async def _init_queue(self) -> None:
        """Initialize queue backend."""
        logger.debug("Connecting to Redis")

        # Import here to avoid circular imports
        from mongoclaw.queue.redis_stream import RedisStreamBackend

        self._queue_backend = RedisStreamBackend(
            url=self._settings.redis.url.get_secret_value(),
            max_connections=self._settings.redis.max_connections,
        )
        await self._queue_backend.connect()
        logger.info("Connected to Redis")

    async def _stop_queue(self) -> None:
        """Stop queue backend."""
        if self._queue_backend:
            await self._queue_backend.disconnect()
            self._queue_backend = None

    async def _init_watcher(self) -> None:
        """Initialize change stream watcher."""
        logger.debug("Initializing change stream watcher")

        from mongoclaw.watcher.change_stream import ChangeStreamWatcher

        self._watcher = ChangeStreamWatcher(
            mongo_client=self._mongo_client,
            agent_store=self._agent_store,
            settings=self._settings,
        )

    async def _stop_watcher(self) -> None:
        """Stop change stream watcher."""
        if self._watcher:
            await self._watcher.stop()
            self._watcher = None

    async def _init_dispatcher(self) -> None:
        """Initialize event dispatcher."""
        logger.debug("Initializing dispatcher")

        from mongoclaw.dispatcher.agent_dispatcher import AgentDispatcher
        from mongoclaw.dispatcher.routing import RoutingStrategy

        self._dispatcher = AgentDispatcher(
            agent_store=self._agent_store,
            queue_backend=self._queue_backend,
            settings=self._settings,
            routing_strategy=RoutingStrategy(self._settings.worker.routing_strategy),
        )

    async def _init_worker_pool(self) -> None:
        """Initialize worker pool."""
        logger.debug("Initializing worker pool")

        from mongoclaw.worker.pool import WorkerPool
        from mongoclaw.dispatcher.routing import RoutingStrategy

        self._worker_pool = WorkerPool(
            queue_backend=self._queue_backend,
            agent_store=self._agent_store,
            settings=self._settings,
            routing_strategy=RoutingStrategy(self._settings.worker.routing_strategy),
            mongo_client=self._mongo_client,
        )

    async def _start_watcher(self) -> None:
        """Start the change stream watcher."""
        if self._watcher:
            logger.debug("Starting change stream watcher")
            # Set dispatcher on watcher
            if self._dispatcher:
                self._watcher.set_dispatcher(self._dispatcher)
            await self._watcher.start()

    async def _start_worker_pool(self) -> None:
        """Start the worker pool."""
        if self._worker_pool:
            logger.debug("Starting worker pool")
            await self._worker_pool.start()

    async def _stop_worker_pool(self) -> None:
        """Stop worker pool gracefully."""
        if self._worker_pool:
            await self._worker_pool.shutdown()
            self._worker_pool = None

    def _register_signal_handlers(self) -> None:
        """Register signal handlers for graceful shutdown."""
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(self._handle_signal(s)),
            )

    async def _handle_signal(self, sig: signal.Signals) -> None:
        """Handle shutdown signal."""
        logger.info("Received signal", signal=sig.name)
        self._shutdown_event.set()

    # Public methods for runtime control

    async def reload_agents(self) -> int:
        """
        Reload agent configurations.

        Returns:
            Number of agents loaded.
        """
        if not self._agent_store:
            raise RuntimeError("Runtime not started")

        agents = await self._agent_store.list(enabled_only=True)
        logger.info("Reloaded agents", count=len(agents))

        # Notify watcher to update its watch targets
        if self._watcher:
            await self._watcher.refresh_watches()

        return len(agents)

    async def get_stats(self) -> dict[str, Any]:
        """
        Get runtime statistics.

        Returns:
            Dictionary of runtime stats.
        """
        stats: dict[str, Any] = {
            "running": self._running,
            "environment": self._settings.environment,
        }

        if self._agent_store:
            stats["agents"] = {
                "total": await self._agent_store.count(),
                "enabled": await self._agent_store.count(enabled_only=True),
            }

        if self._queue_backend:
            # Add queue stats
            pass

        if self._worker_pool:
            # Add worker stats
            pass

        return stats


# Singleton runtime instance
_runtime: Runtime | None = None


def get_runtime() -> Runtime:
    """Get the global runtime instance."""
    global _runtime
    if _runtime is None:
        _runtime = Runtime()
    return _runtime


def configure_runtime(settings: Settings) -> Runtime:
    """Configure and return the global runtime instance."""
    global _runtime
    _runtime = Runtime(settings)
    return _runtime
