"""Consumer group management for Redis Streams."""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

from mongoclaw.observability.logging import get_logger
from mongoclaw.queue.redis_stream import RedisStreamBackend

logger = get_logger(__name__)


class ConsumerGroupManager:
    """
    Manages consumer groups for distributed work processing.

    Handles:
    - Consumer registration and health
    - Pending message recovery
    - Consumer rebalancing
    """

    def __init__(
        self,
        backend: RedisStreamBackend,
        group_name: str = "mongoclaw-workers",
        consumer_prefix: str | None = None,
        claim_interval_seconds: float = 30.0,
        min_idle_ms: int = 60000,
    ) -> None:
        self._backend = backend
        self._group_name = group_name

        # Generate unique consumer name
        hostname = os.environ.get("HOSTNAME", "local")
        self._consumer_prefix = consumer_prefix or f"{hostname}-{uuid.uuid4().hex[:8]}"

        self._claim_interval = claim_interval_seconds
        self._min_idle_ms = min_idle_ms

        self._consumers: dict[str, str] = {}  # stream -> consumer_name
        self._claim_task: asyncio.Task[None] | None = None
        self._running = False

    @property
    def group_name(self) -> str:
        """Get the consumer group name."""
        return self._group_name

    def get_consumer_name(self, stream_name: str) -> str:
        """
        Get or create a consumer name for a stream.

        Args:
            stream_name: The stream name.

        Returns:
            Consumer name for this stream.
        """
        if stream_name not in self._consumers:
            # Create unique consumer name per stream
            short_stream = stream_name.split(":")[-1][:8]
            self._consumers[stream_name] = f"{self._consumer_prefix}-{short_stream}"

        return self._consumers[stream_name]

    async def start(self) -> None:
        """Start the consumer group manager."""
        if self._running:
            return

        self._running = True

        # Start pending message claimer
        self._claim_task = asyncio.create_task(
            self._claim_loop(),
            name="consumer_group_claim",
        )

        logger.info(
            "Consumer group manager started",
            group=self._group_name,
            prefix=self._consumer_prefix,
        )

    async def stop(self) -> None:
        """Stop the consumer group manager."""
        self._running = False

        if self._claim_task:
            self._claim_task.cancel()
            try:
                await self._claim_task
            except asyncio.CancelledError:
                pass
            self._claim_task = None

        logger.info("Consumer group manager stopped")

    async def _claim_loop(self) -> None:
        """Periodically claim pending messages from dead consumers."""
        while self._running:
            try:
                await asyncio.sleep(self._claim_interval)

                for stream_name, consumer_name in self._consumers.items():
                    try:
                        claimed = await self._backend.claim_pending(
                            stream_name=stream_name,
                            consumer_group=self._group_name,
                            consumer_name=consumer_name,
                            min_idle_ms=self._min_idle_ms,
                            count=10,
                        )

                        if claimed:
                            logger.info(
                                "Claimed pending messages",
                                stream=stream_name,
                                count=len(claimed),
                            )

                    except Exception as e:
                        logger.warning(
                            "Failed to claim pending",
                            stream=stream_name,
                            error=str(e),
                        )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Error in claim loop", error=str(e))

    async def get_group_stats(self, stream_name: str) -> dict[str, Any]:
        """
        Get statistics for a consumer group on a stream.

        Args:
            stream_name: The stream name.

        Returns:
            Dictionary of group statistics.
        """
        try:
            pending = await self._backend.get_pending_count(
                stream_name, self._group_name
            )
            consumers = await self._backend.get_consumer_info(
                stream_name, self._group_name
            )

            return {
                "group_name": self._group_name,
                "pending_count": pending,
                "consumer_count": len(consumers),
                "consumers": consumers,
            }
        except Exception:
            return {
                "group_name": self._group_name,
                "error": "Failed to get stats",
            }

    async def register_stream(self, stream_name: str) -> str:
        """
        Register this manager for a stream.

        Args:
            stream_name: The stream name.

        Returns:
            The consumer name for this stream.
        """
        consumer_name = self.get_consumer_name(stream_name)
        logger.debug(
            "Registered for stream",
            stream=stream_name,
            consumer=consumer_name,
        )
        return consumer_name

    async def unregister_stream(self, stream_name: str) -> None:
        """
        Unregister from a stream.

        Args:
            stream_name: The stream name.
        """
        if stream_name in self._consumers:
            del self._consumers[stream_name]
            logger.debug("Unregistered from stream", stream=stream_name)
