"""Hot reload support for agent configurations."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Callable, Awaitable

import structlog
from motor.motor_asyncio import AsyncIOMotorClient

from mongoclaw.agents.models import AgentConfig
from mongoclaw.agents.store import AgentStore

logger = structlog.get_logger(__name__)


class AgentHotReloader:
    """Watches for agent configuration changes and reloads them."""

    def __init__(
        self,
        client: AsyncIOMotorClient,
        database: str,
        collection: str,
        on_agent_created: Callable[[AgentConfig], Awaitable[None]] | None = None,
        on_agent_updated: Callable[[AgentConfig], Awaitable[None]] | None = None,
        on_agent_deleted: Callable[[str], Awaitable[None]] | None = None,
        poll_interval: float = 5.0,
    ) -> None:
        """Initialize the hot reloader.

        Args:
            client: MongoDB client.
            database: Database name.
            collection: Collection name for agents.
            on_agent_created: Callback when agent is created.
            on_agent_updated: Callback when agent is updated.
            on_agent_deleted: Callback when agent is deleted.
            poll_interval: Seconds between change checks (fallback).
        """
        self._client = client
        self._database = database
        self._collection = collection
        self._on_created = on_agent_created
        self._on_updated = on_agent_updated
        self._on_deleted = on_agent_deleted
        self._poll_interval = poll_interval

        self._store = AgentStore(
            client=client,
            database=database,
            collection=collection,
        )

        self._running = False
        self._task: asyncio.Task | None = None
        self._agents_cache: dict[str, AgentConfig] = {}
        self._last_check: datetime | None = None

    async def start(self) -> None:
        """Start watching for changes."""
        if self._running:
            return

        self._running = True

        # Load initial state
        await self._load_initial_state()

        # Try change stream first, fall back to polling
        self._task = asyncio.create_task(self._watch_changes())

        logger.info("Agent hot reloader started")

    async def stop(self) -> None:
        """Stop watching for changes."""
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        logger.info("Agent hot reloader stopped")

    async def _load_initial_state(self) -> None:
        """Load current agents into cache."""
        agents = await self._store.list(enabled_only=False)
        self._agents_cache = {agent.id: agent for agent in agents}
        self._last_check = datetime.utcnow()

        logger.info(
            "Loaded initial agent state",
            agent_count=len(self._agents_cache),
        )

    async def _watch_changes(self) -> None:
        """Watch for changes using change stream or polling."""
        try:
            # Try change stream first
            await self._watch_with_change_stream()
        except Exception as e:
            logger.warning(
                "Change stream not available, falling back to polling",
                error=str(e),
            )
            await self._watch_with_polling()

    async def _watch_with_change_stream(self) -> None:
        """Watch for changes using MongoDB change stream."""
        db = self._client[self._database]
        collection = db[self._collection]

        pipeline = [
            {"$match": {"operationType": {"$in": ["insert", "update", "replace", "delete"]}}}
        ]

        async with collection.watch(pipeline) as stream:
            logger.info("Using change stream for hot reload")

            async for change in stream:
                if not self._running:
                    break

                await self._handle_change_event(change)

    async def _handle_change_event(self, change: dict) -> None:
        """Handle a change stream event."""
        operation = change.get("operationType")
        document_key = change.get("documentKey", {})
        agent_id = document_key.get("_id")

        if not agent_id:
            return

        agent_id = str(agent_id)

        try:
            if operation == "delete":
                if agent_id in self._agents_cache:
                    del self._agents_cache[agent_id]
                    if self._on_deleted:
                        await self._on_deleted(agent_id)
                    logger.info("Agent deleted", agent_id=agent_id)

            elif operation in ("insert", "update", "replace"):
                # Fetch the full document
                agent = await self._store.get_optional(agent_id)

                if agent:
                    was_new = agent_id not in self._agents_cache
                    self._agents_cache[agent_id] = agent

                    if was_new:
                        if self._on_created:
                            await self._on_created(agent)
                        logger.info("Agent created", agent_id=agent_id)
                    else:
                        if self._on_updated:
                            await self._on_updated(agent)
                        logger.info("Agent updated", agent_id=agent_id)

        except Exception as e:
            logger.error(
                "Error handling agent change",
                agent_id=agent_id,
                operation=operation,
                error=str(e),
            )

    async def _watch_with_polling(self) -> None:
        """Watch for changes using polling."""
        logger.info("Using polling for hot reload", interval=self._poll_interval)

        while self._running:
            try:
                await self._check_for_changes()
            except Exception as e:
                logger.error("Error checking for changes", error=str(e))

            await asyncio.sleep(self._poll_interval)

    async def _check_for_changes(self) -> None:
        """Check for agent changes by comparing with cache."""
        current_agents = await self._store.list(enabled_only=False)
        current_map = {agent.id: agent for agent in current_agents}

        # Check for new and updated agents
        for agent_id, agent in current_map.items():
            if agent_id not in self._agents_cache:
                self._agents_cache[agent_id] = agent
                if self._on_created:
                    await self._on_created(agent)
                logger.info("Agent created (poll)", agent_id=agent_id)

            elif self._has_changed(self._agents_cache[agent_id], agent):
                self._agents_cache[agent_id] = agent
                if self._on_updated:
                    await self._on_updated(agent)
                logger.info("Agent updated (poll)", agent_id=agent_id)

        # Check for deleted agents
        deleted_ids = set(self._agents_cache.keys()) - set(current_map.keys())
        for agent_id in deleted_ids:
            del self._agents_cache[agent_id]
            if self._on_deleted:
                await self._on_deleted(agent_id)
            logger.info("Agent deleted (poll)", agent_id=agent_id)

        self._last_check = datetime.utcnow()

    def _has_changed(self, old: AgentConfig, new: AgentConfig) -> bool:
        """Check if an agent configuration has changed."""
        # Compare updated_at timestamps if available
        if old.updated_at and new.updated_at:
            return new.updated_at > old.updated_at

        # Fall back to comparing serialized forms
        return old.model_dump() != new.model_dump()

    def get_agent(self, agent_id: str) -> AgentConfig | None:
        """Get a cached agent by ID."""
        return self._agents_cache.get(agent_id)

    def get_all_agents(self) -> list[AgentConfig]:
        """Get all cached agents."""
        return list(self._agents_cache.values())

    def get_enabled_agents(self) -> list[AgentConfig]:
        """Get all enabled cached agents."""
        return [agent for agent in self._agents_cache.values() if agent.enabled]

    @property
    def agent_count(self) -> int:
        """Get the number of cached agents."""
        return len(self._agents_cache)
