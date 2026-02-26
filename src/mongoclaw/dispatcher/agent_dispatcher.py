"""Agent dispatcher for routing events to queue."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

from mongoclaw.core.config import Settings
from mongoclaw.core.types import ChangeEvent
from mongoclaw.dispatcher.routing import RoutingStrategy, get_dlq_stream_name, get_stream_name
from mongoclaw.dispatcher.work_item import WorkItem
from mongoclaw.observability.logging import get_logger
from mongoclaw.observability.metrics import get_metrics_collector

if TYPE_CHECKING:
    from mongoclaw.agents.models import AgentConfig
    from mongoclaw.agents.store import AgentStore
    from mongoclaw.core.types import QueueBackend

logger = get_logger(__name__)


class AgentDispatcher:
    """
    Dispatches change events to the work queue for processing.

    Responsibilities:
    - Creates work items from change events
    - Applies routing strategy for stream selection
    - Handles deduplication
    - Tracks dispatch metrics
    """

    def __init__(
        self,
        agent_store: AgentStore,
        queue_backend: QueueBackend,
        settings: Settings,
        routing_strategy: RoutingStrategy = RoutingStrategy.BY_AGENT,
    ) -> None:
        self._agent_store = agent_store
        self._queue = queue_backend
        self._settings = settings
        self._routing_strategy = routing_strategy

        # Deduplication cache (in-memory, for quick checks)
        self._recent_keys: set[str] = set()
        self._max_cache_size = 10000

        # Metrics
        self._dispatched_count = 0
        self._deduplicated_count = 0
        self._dropped_count = 0
        self._deferred_count = 0
        self._dlq_count = 0
        self._forced_enqueue_count = 0
        self._pressure_cache: dict[str, tuple[float, float]] = {}

    async def dispatch(
        self,
        agent: AgentConfig,
        event: ChangeEvent,
    ) -> str | None:
        """
        Dispatch a change event for processing by an agent.

        Args:
            agent: The agent configuration.
            event: The change event.

        Returns:
            Work item ID if dispatched, None if deduplicated.
        """
        # Create work item
        work_item = WorkItem.from_event(
            agent_id=agent.id,
            event=event,
            max_attempts=agent.execution.max_retries + 1,
            priority=agent.execution.priority,
        )

        # Generate idempotency key if deduplication enabled
        if agent.execution.deduplicate:
            key = self._generate_idempotency_key(agent, work_item)
            work_item.idempotency_key = key

            # Check for duplicate
            if self._is_duplicate(key):
                logger.debug(
                    "Deduplicated work item",
                    agent_id=agent.id,
                    document_id=work_item.document_id,
                    idempotency_key=key,
                )
                self._deduplicated_count += 1
                return None

            # Add to recent keys
            self._add_to_cache(key)

        # Determine target stream
        stream_name = get_stream_name(
            agent=agent,
            work_item=work_item,
            strategy=self._routing_strategy,
            num_partitions=self._settings.worker.routing_partition_count,
        )
        self._annotate_delivery_metadata(work_item, stream_name)

        should_enqueue = await self._apply_backpressure_admission(
            agent=agent,
            work_item=work_item,
            stream_name=stream_name,
        )
        if not should_enqueue:
            return None

        # Enqueue
        message_id = await self._queue.enqueue(work_item, stream_name)
        get_metrics_collector().record_dispatch_routed(
            strategy=self._routing_strategy.value,
            stream=stream_name,
        )

        logger.info(
            "Dispatched work item",
            work_item_id=work_item.id,
            agent_id=agent.id,
            document_id=work_item.document_id,
            stream=stream_name,
            message_id=message_id,
        )

        self._dispatched_count += 1
        return work_item.id

    async def dispatch_batch(
        self,
        items: list[tuple[AgentConfig, ChangeEvent]],
    ) -> list[str]:
        """
        Dispatch multiple events.

        Args:
            items: List of (agent, event) tuples.

        Returns:
            List of dispatched work item IDs.
        """
        dispatched = []

        for agent, event in items:
            work_item_id = await self.dispatch(agent, event)
            if work_item_id:
                dispatched.append(work_item_id)

        return dispatched

    def _annotate_delivery_metadata(self, work_item: WorkItem, stream_name: str) -> None:
        """Attach explicit delivery/routing semantics to work item metadata."""
        work_item.metadata["delivery_semantics"] = "at_least_once"
        work_item.metadata["routing_strategy"] = self._routing_strategy.value
        work_item.metadata["stream"] = stream_name
        if stream_name.startswith("mongoclaw:partition:"):
            try:
                work_item.metadata["partition"] = int(stream_name.rsplit(":", 1)[1])
            except ValueError:
                pass

    async def _apply_backpressure_admission(
        self,
        agent: AgentConfig,
        work_item: WorkItem,
        stream_name: str,
    ) -> bool:
        """Apply priority-aware backpressure policy before enqueue."""
        worker_settings = self._settings.worker
        if not worker_settings.dispatch_backpressure_enabled:
            return True

        threshold = worker_settings.dispatch_backpressure_threshold
        fullness = await self._get_stream_fullness(stream_name)
        get_metrics_collector().set_dispatch_queue_fullness(stream_name, fullness)
        if fullness < threshold:
            return True

        if work_item.priority >= worker_settings.dispatch_min_priority_when_backpressured:
            get_metrics_collector().record_dispatch_admission(
                agent.id,
                stream_name,
                "priority_bypass",
            )
            logger.info(
                "Priority bypass under backpressure",
                agent_id=agent.id,
                stream=stream_name,
                priority=work_item.priority,
                fullness=round(fullness, 3),
            )
            return True

        policy = worker_settings.dispatch_overflow_policy
        if policy == "drop":
            self._dropped_count += 1
            get_metrics_collector().record_dispatch_admission(agent.id, stream_name, "drop")
            logger.warning(
                "Dropped work item due to backpressure",
                agent_id=agent.id,
                stream=stream_name,
                priority=work_item.priority,
                fullness=round(fullness, 3),
            )
            return False

        if policy == "dlq":
            self._dlq_count += 1
            get_metrics_collector().record_dispatch_admission(agent.id, stream_name, "dlq")
            await self._queue.move_to_dlq(
                work_item,
                Exception("Dispatch backpressure overflow"),
                get_dlq_stream_name(agent),
            )
            logger.warning(
                "Sent work item to DLQ due to backpressure",
                agent_id=agent.id,
                stream=stream_name,
                priority=work_item.priority,
                fullness=round(fullness, 3),
            )
            return False

        self._deferred_count += 1
        get_metrics_collector().record_dispatch_admission(agent.id, stream_name, "defer")
        for _ in range(worker_settings.dispatch_defer_max_attempts):
            await asyncio.sleep(worker_settings.dispatch_defer_seconds)
            fullness = await self._get_stream_fullness(stream_name)
            get_metrics_collector().set_dispatch_queue_fullness(stream_name, fullness)
            if fullness < threshold:
                return True

        self._forced_enqueue_count += 1
        get_metrics_collector().record_dispatch_admission(
            agent.id,
            stream_name,
            "defer_forced_enqueue",
        )
        logger.warning(
            "Forced enqueue after defer attempts",
            agent_id=agent.id,
            stream=stream_name,
            priority=work_item.priority,
            fullness=round(fullness, 3),
        )
        return True

    async def _get_stream_fullness(self, stream_name: str) -> float:
        """Get stream fullness ratio with short cache to reduce Redis calls."""
        ttl = self._settings.worker.dispatch_pressure_cache_ttl_seconds
        now = time.monotonic()
        cached = self._pressure_cache.get(stream_name)
        if cached and (now - cached[0]) < ttl:
            return cached[1]

        capacity = max(1, self._settings.redis.stream_max_len)
        length = await self._queue.get_stream_length(stream_name)
        fullness = min(1.0, float(length) / float(capacity))
        self._pressure_cache[stream_name] = (now, fullness)
        return fullness

    def _generate_idempotency_key(
        self,
        agent: AgentConfig,
        work_item: WorkItem,
    ) -> str:
        """
        Generate an idempotency key for a work item.

        The key format can be customized via agent.write.idempotency_key template.
        """
        if agent.write.idempotency_key:
            # TODO: Render template with document context
            # For now, fall back to default generation
            pass

        return work_item.generate_idempotency_key()

    def _is_duplicate(self, key: str) -> bool:
        """Check if a key has been seen recently."""
        return key in self._recent_keys

    def _add_to_cache(self, key: str) -> None:
        """Add a key to the deduplication cache."""
        self._recent_keys.add(key)

        # Trim cache if too large
        if len(self._recent_keys) > self._max_cache_size:
            # Remove oldest entries (convert to list, remove first half)
            keys_list = list(self._recent_keys)
            self._recent_keys = set(keys_list[self._max_cache_size // 2 :])

    def get_stats(self) -> dict[str, Any]:
        """Get dispatcher statistics."""
        return {
            "dispatched_count": self._dispatched_count,
            "deduplicated_count": self._deduplicated_count,
            "dropped_count": self._dropped_count,
            "deferred_count": self._deferred_count,
            "dlq_count": self._dlq_count,
            "forced_enqueue_count": self._forced_enqueue_count,
            "cache_size": len(self._recent_keys),
            "routing_strategy": self._routing_strategy.value,
        }

    def clear_cache(self) -> None:
        """Clear the deduplication cache."""
        self._recent_keys.clear()
