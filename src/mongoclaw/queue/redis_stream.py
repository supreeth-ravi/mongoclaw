"""Redis Streams implementation of queue backend."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import redis.asyncio as redis
from redis.exceptions import ResponseError

from mongoclaw.core.exceptions import QueueConnectionError, QueueError
from mongoclaw.dispatcher.work_item import WorkItem
from mongoclaw.observability.logging import get_logger
from mongoclaw.queue.base import QueueBackendBase
from mongoclaw.queue.serialization import deserialize_work_item, serialize_work_item

logger = get_logger(__name__)


class RedisStreamBackend(QueueBackendBase):
    """
    Redis Streams implementation of the queue backend.

    Features:
    - Consumer groups for distributed processing
    - At-least-once delivery semantics
    - Automatic stream trimming
    - Pending message recovery
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        max_connections: int = 50,
        socket_timeout: float = 5.0,
        stream_max_len: int = 100000,
        decode_responses: bool = True,
    ) -> None:
        self._url = url
        self._max_connections = max_connections
        self._socket_timeout = socket_timeout
        self._stream_max_len = stream_max_len
        self._decode_responses = decode_responses

        self._pool: redis.ConnectionPool | None = None
        self._client: redis.Redis[str] | None = None
        self._connected = False

    @property
    def client(self) -> redis.Redis[str]:
        """Get the Redis client."""
        if self._client is None:
            raise RuntimeError("Not connected to Redis")
        return self._client

    async def connect(self) -> None:
        """Connect to Redis."""
        if self._connected:
            return

        try:
            self._pool = redis.ConnectionPool.from_url(
                self._url,
                max_connections=self._max_connections,
                socket_timeout=self._socket_timeout,
                decode_responses=self._decode_responses,
            )
            self._client = redis.Redis(connection_pool=self._pool)

            # Test connection
            await self._client.ping()

            self._connected = True
            logger.info("Connected to Redis")

        except Exception as e:
            raise QueueConnectionError(
                f"Failed to connect to Redis: {e}",
                details={"url": self._url.split("@")[-1]},  # Hide password
            )

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._client:
            await self._client.close()
            self._client = None

        if self._pool:
            await self._pool.disconnect()
            self._pool = None

        self._connected = False
        logger.info("Disconnected from Redis")

    async def enqueue(self, work_item: WorkItem, stream_name: str) -> str:
        """Enqueue a work item to a stream."""
        try:
            data = serialize_work_item(work_item)

            message_id = await self.client.xadd(
                stream_name,
                {"data": data},
                maxlen=self._stream_max_len,
                approximate=True,
            )

            logger.debug(
                "Enqueued work item",
                stream=stream_name,
                message_id=message_id,
                work_item_id=work_item.id,
            )

            return str(message_id)

        except Exception as e:
            raise QueueError(
                f"Failed to enqueue work item: {e}",
                details={"stream": stream_name, "work_item_id": work_item.id},
            )

    async def dequeue(
        self,
        stream_name: str,
        consumer_group: str,
        consumer_name: str,
        count: int = 1,
        block_ms: int = 5000,
    ) -> list[tuple[str, WorkItem]]:
        """Dequeue work items from a stream."""
        try:
            # Ensure consumer group exists
            await self._ensure_consumer_group(stream_name, consumer_group)

            # Read from stream
            result = await self.client.xreadgroup(
                groupname=consumer_group,
                consumername=consumer_name,
                streams={stream_name: ">"},
                count=count,
                block=block_ms,
            )

            if not result:
                return []

            items: list[tuple[str, WorkItem]] = []

            for stream_data in result:
                stream_key, messages = stream_data

                for message_id, data in messages:
                    try:
                        work_item = deserialize_work_item(data.get("data", ""))
                        items.append((str(message_id), work_item))

                    except Exception as e:
                        logger.warning(
                            "Failed to deserialize work item",
                            message_id=message_id,
                            error=str(e),
                        )
                        # Ack bad messages to avoid infinite loop
                        await self.ack(stream_name, consumer_group, str(message_id))

            return items

        except ResponseError as e:
            if "NOGROUP" in str(e):
                # Group doesn't exist, create it
                await self._ensure_consumer_group(stream_name, consumer_group)
                return []
            raise QueueError(f"Failed to dequeue: {e}")

        except Exception as e:
            raise QueueError(f"Failed to dequeue: {e}")

    async def ack(
        self,
        stream_name: str,
        consumer_group: str,
        message_id: str,
    ) -> None:
        """Acknowledge a message."""
        try:
            await self.client.xack(stream_name, consumer_group, message_id)
            logger.debug(
                "Acknowledged message",
                stream=stream_name,
                message_id=message_id,
            )
        except Exception as e:
            logger.warning(
                "Failed to acknowledge message",
                stream=stream_name,
                message_id=message_id,
                error=str(e),
            )

    async def move_to_dlq(
        self,
        work_item: WorkItem,
        error: Exception,
        dlq_stream: str,
    ) -> str:
        """Move a work item to the dead letter queue."""
        # Add error info to metadata
        work_item.metadata["dlq_error"] = str(error)
        work_item.metadata["dlq_error_type"] = type(error).__name__
        work_item.metadata["dlq_timestamp"] = datetime.utcnow().isoformat()

        message_id = await self.enqueue(work_item, dlq_stream)

        logger.info(
            "Moved to DLQ",
            work_item_id=work_item.id,
            dlq_stream=dlq_stream,
            error=str(error),
        )

        return message_id

    async def get_pending_count(
        self,
        stream_name: str,
        consumer_group: str,
    ) -> int:
        """Get count of pending messages."""
        try:
            info = await self.client.xpending(stream_name, consumer_group)
            if info:
                return info["pending"]
            return 0
        except ResponseError:
            return 0

    async def get_stream_length(self, stream_name: str) -> int:
        """Get the length of a stream."""
        try:
            return await self.client.xlen(stream_name)
        except ResponseError:
            return 0

    async def health_check(self) -> bool:
        """Check Redis health."""
        try:
            await self.client.ping()
            return True
        except Exception:
            return False

    async def _ensure_consumer_group(
        self,
        stream_name: str,
        consumer_group: str,
    ) -> None:
        """Ensure a consumer group exists."""
        try:
            await self.client.xgroup_create(
                stream_name,
                consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info(
                "Created consumer group",
                stream=stream_name,
                group=consumer_group,
            )
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def claim_pending(
        self,
        stream_name: str,
        consumer_group: str,
        consumer_name: str,
        min_idle_ms: int = 60000,
        count: int = 10,
    ) -> list[tuple[str, WorkItem]]:
        """
        Claim pending messages that have been idle too long.

        This recovers messages from crashed consumers.
        """
        try:
            # Get pending messages
            pending = await self.client.xpending_range(
                stream_name,
                consumer_group,
                min="-",
                max="+",
                count=count,
            )

            if not pending:
                return []

            # Filter by idle time
            message_ids = [
                p["message_id"]
                for p in pending
                if p["time_since_delivered"] >= min_idle_ms
            ]

            if not message_ids:
                return []

            # Claim messages
            claimed = await self.client.xclaim(
                stream_name,
                consumer_group,
                consumer_name,
                min_idle_time=min_idle_ms,
                message_ids=message_ids,
            )

            items: list[tuple[str, WorkItem]] = []

            for message_id, data in claimed:
                try:
                    work_item = deserialize_work_item(data.get("data", ""))
                    # Increment attempt since this is a retry
                    work_item = work_item.increment_attempt()
                    items.append((str(message_id), work_item))
                except Exception:
                    pass

            return items

        except Exception as e:
            logger.warning("Failed to claim pending messages", error=str(e))
            return []

    async def get_stats(self) -> dict[str, Any]:
        """Get queue statistics."""
        try:
            info = await self.client.info("memory")
            return {
                "connected": self._connected,
                "used_memory": info.get("used_memory_human", "unknown"),
                "connected_clients": info.get("connected_clients", 0),
            }
        except Exception:
            return {"connected": self._connected}

    async def delete_stream(self, stream_name: str) -> bool:
        """Delete a stream (for testing/cleanup)."""
        try:
            result = await self.client.delete(stream_name)
            return result > 0
        except Exception:
            return False

    async def get_consumer_info(
        self,
        stream_name: str,
        consumer_group: str,
    ) -> list[dict[str, Any]]:
        """Get information about consumers in a group."""
        try:
            consumers = await self.client.xinfo_consumers(stream_name, consumer_group)
            return [
                {
                    "name": c["name"],
                    "pending": c["pending"],
                    "idle": c["idle"],
                }
                for c in consumers
            ]
        except ResponseError:
            return []
