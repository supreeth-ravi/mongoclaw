"""MongoDB change stream watcher implementation."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorChangeStream
from pymongo.errors import PyMongoError

from mongoclaw.core.config import Settings
from mongoclaw.core.types import ChangeEvent, ChangeOperation
from mongoclaw.observability.logging import get_logger
from mongoclaw.watcher.event_matcher import EventMatcher
from mongoclaw.watcher.resume_token import ResumeTokenStore

if TYPE_CHECKING:
    from mongoclaw.agents.store import AgentStore

logger = get_logger(__name__)


class ChangeStreamWatcher:
    """
    Watches MongoDB change streams for events matching agent configurations.

    Features:
    - Watches multiple databases/collections based on agent configs
    - Persists resume tokens for crash recovery
    - Matches events to agents and dispatches to queue
    - Supports graceful shutdown
    """

    def __init__(
        self,
        mongo_client: AsyncIOMotorClient[dict[str, Any]],
        agent_store: AgentStore,
        settings: Settings,
        dispatcher: Any | None = None,  # AgentDispatcher
    ) -> None:
        self._client = mongo_client
        self._agent_store = agent_store
        self._settings = settings
        self._dispatcher = dispatcher

        self._token_store = ResumeTokenStore(
            client=mongo_client,
            database=settings.mongodb.database,
            collection=settings.mongodb.resume_tokens_collection,
        )
        self._matcher = EventMatcher(agent_store)

        self._running = False
        self._streams: dict[str, AsyncIOMotorChangeStream[dict[str, Any]]] = {}
        self._watch_tasks: dict[str, asyncio.Task[None]] = {}
        self._refresh_task: asyncio.Task[None] | None = None
        self._agent_watch_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    @property
    def is_running(self) -> bool:
        """Check if watcher is running."""
        return self._running

    def set_dispatcher(self, dispatcher: Any) -> None:
        """Set the dispatcher for sending matched events."""
        self._dispatcher = dispatcher

    async def start(self) -> None:
        """Start watching all configured change streams."""
        if self._running:
            logger.warning("Watcher already running")
            return

        logger.info("Starting change stream watcher")
        self._running = True
        self._stop_event.clear()

        # Initialize token store
        await self._token_store.initialize()

        # Start watches for all configured targets
        await self.refresh_watches()
        self._refresh_task = asyncio.create_task(
            self._refresh_loop(),
            name="watch_refresh_loop",
        )
        self._agent_watch_task = asyncio.create_task(
            self._watch_agent_configs(),
            name="agent_config_watch_loop",
        )

        logger.info("Change stream watcher started")

    async def stop(self) -> None:
        """Stop all change stream watches."""
        if not self._running:
            return

        logger.info("Stopping change stream watcher")
        self._running = False
        self._stop_event.set()

        # Cancel all watch tasks
        for task in self._watch_tasks.values():
            task.cancel()

        if self._watch_tasks:
            await asyncio.gather(*self._watch_tasks.values(), return_exceptions=True)

        if self._refresh_task is not None:
            self._refresh_task.cancel()
            await asyncio.gather(self._refresh_task, return_exceptions=True)
            self._refresh_task = None

        if self._agent_watch_task is not None:
            self._agent_watch_task.cancel()
            await asyncio.gather(self._agent_watch_task, return_exceptions=True)
            self._agent_watch_task = None

        # Close all streams
        for stream in self._streams.values():
            await stream.close()

        self._streams.clear()
        self._watch_tasks.clear()

        logger.info("Change stream watcher stopped")

    async def refresh_watches(self) -> None:
        """Refresh watch targets based on current agent configurations."""
        targets = await self._agent_store.get_all_watch_targets(enabled_only=True)

        current_targets = set(self._watch_tasks.keys())
        new_targets = {f"{db}.{coll}" for db, coll in targets}

        # Stop watches that are no longer needed
        to_remove = current_targets - new_targets
        for ns in to_remove:
            await self._stop_watch(ns)

        # Start new watches
        to_add = new_targets - current_targets
        for ns in to_add:
            db, coll = ns.split(".", 1)
            await self._start_watch(db, coll)

        log_fn = logger.info if to_add or to_remove else logger.debug
        log_fn(
            "Refreshed watches",
            total=len(new_targets),
            added=len(to_add),
            removed=len(to_remove),
        )

    async def _refresh_loop(self) -> None:
        """Periodically refresh watch targets while runtime is running."""
        while self._running:
            try:
                await asyncio.sleep(5)
                await self.refresh_watches()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Watch refresh loop error", error=str(e))

    async def _watch_agent_configs(self) -> None:
        """Watch the agents collection and refresh watches on config changes."""
        db_name = self._settings.mongodb.database
        coll_name = self._settings.mongodb.agents_collection
        ns = f"{db_name}.{coll_name}"
        coll = self._client[db_name][coll_name]

        while self._running:
            try:
                async with coll.watch() as stream:
                    logger.info("Agent config watch opened", namespace=ns)
                    async for _ in stream:
                        if not self._running:
                            break
                        await self.refresh_watches()
            except asyncio.CancelledError:
                break
            except PyMongoError as e:
                logger.warning("Agent config watch error", namespace=ns, error=str(e))
                await asyncio.sleep(1)
            except Exception as e:
                logger.warning("Agent config watch loop error", namespace=ns, error=str(e))
                await asyncio.sleep(1)

    async def _start_watch(self, database: str, collection: str) -> None:
        """Start watching a specific collection."""
        ns = f"{database}.{collection}"

        if ns in self._watch_tasks:
            return

        logger.debug("Starting watch", database=database, collection=collection)

        # Get resume token if available
        resume_token = await self._token_store.get(database, collection)

        # Create watch task
        task = asyncio.create_task(
            self._watch_loop(database, collection, resume_token),
            name=f"watch_{ns}",
        )
        self._watch_tasks[ns] = task

    async def _stop_watch(self, namespace: str) -> None:
        """Stop watching a specific namespace."""
        if namespace not in self._watch_tasks:
            return

        logger.debug("Stopping watch", namespace=namespace)

        task = self._watch_tasks.pop(namespace)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        if namespace in self._streams:
            await self._streams[namespace].close()
            del self._streams[namespace]

    async def _watch_loop(
        self,
        database: str,
        collection: str,
        resume_token: dict[str, Any] | None = None,
    ) -> None:
        """Main watch loop for a collection."""
        ns = f"{database}.{collection}"
        coll = self._client[database][collection]

        # Build pipeline for full document lookup
        pipeline: list[dict[str, Any]] = []

        # Watch options
        options: dict[str, Any] = {
            "full_document": "updateLookup",
            "full_document_before_change": "whenAvailable",
        }

        if resume_token:
            options["resume_after"] = resume_token
            logger.info("Resuming watch from token", namespace=ns)

        retry_count = 0
        max_retries = 5
        base_delay = 1.0

        while self._running:
            try:
                async with coll.watch(pipeline, **options) as stream:
                    self._streams[ns] = stream
                    retry_count = 0  # Reset on successful connection

                    logger.info("Change stream opened", namespace=ns)

                    async for change in stream:
                        if not self._running:
                            break

                        await self._handle_change(change, database, collection)

            except asyncio.CancelledError:
                break

            except PyMongoError as e:
                retry_count += 1

                if retry_count > max_retries:
                    logger.error(
                        "Max retries exceeded for change stream",
                        namespace=ns,
                        error=str(e),
                    )
                    break

                delay = min(base_delay * (2 ** retry_count), 60.0)
                logger.warning(
                    "Change stream error, retrying",
                    namespace=ns,
                    error=str(e),
                    retry_count=retry_count,
                    delay=delay,
                )

                await asyncio.sleep(delay)

                # Try to resume from last saved token
                resume_token = await self._token_store.get(database, collection)
                if resume_token:
                    options["resume_after"] = resume_token

            except Exception as e:
                logger.exception(
                    "Unexpected error in change stream",
                    namespace=ns,
                    error=str(e),
                )
                break

        # Clean up
        if ns in self._streams:
            del self._streams[ns]

    async def _handle_change(
        self,
        change: dict[str, Any],
        database: str,
        collection: str,
    ) -> None:
        """Handle a single change event."""
        try:
            # Parse change event
            event = self._parse_change_event(change, database, collection)

            logger.debug(
                "Received change event",
                operation=event.operation.value,
                namespace=event.namespace,
                document_id=event.document_id,
            )

            # Save resume token
            if event.resume_token:
                await self._token_store.save(database, collection, event.resume_token)

            # Match to agents
            matched_agents = await self._matcher.match(event)

            if not matched_agents:
                logger.debug("No agents matched", document_id=event.document_id)
                return

            logger.info(
                "Matched agents",
                document_id=event.document_id,
                agent_count=len(matched_agents),
                agents=[a.id for a in matched_agents],
            )

            # Dispatch to queue
            if self._dispatcher:
                for agent in matched_agents:
                    await self._dispatcher.dispatch(agent, event)

        except Exception as e:
            logger.exception(
                "Error handling change event",
                error=str(e),
                change=change,
            )

    def _parse_change_event(
        self,
        change: dict[str, Any],
        database: str,
        collection: str,
    ) -> ChangeEvent:
        """Parse a raw MongoDB change event into a ChangeEvent."""
        operation_type = change.get("operationType", "")

        try:
            operation = ChangeOperation(operation_type)
        except ValueError:
            # Handle unknown operations as updates
            operation = ChangeOperation.UPDATE

        return ChangeEvent(
            operation=operation,
            database=database,
            collection=collection,
            document_key=change.get("documentKey", {}),
            full_document=change.get("fullDocument"),
            update_description=change.get("updateDescription"),
            resume_token=change.get("_id"),
            cluster_time=change.get("clusterTime"),
        )
