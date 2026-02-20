"""Dead letter queue handling."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from mongoclaw.dispatcher.work_item import WorkItem
from mongoclaw.observability.logging import get_logger
from mongoclaw.queue.redis_stream import RedisStreamBackend
from mongoclaw.queue.serialization import deserialize_work_item

logger = get_logger(__name__)


class DeadLetterQueue:
    """
    Manages dead letter queue operations.

    Work items are moved to the DLQ when:
    - Maximum retries are exceeded
    - Unrecoverable errors occur
    - Manual intervention is needed
    """

    DEFAULT_STREAM = "mongoclaw:dlq"

    def __init__(
        self,
        backend: RedisStreamBackend,
        stream_name: str = DEFAULT_STREAM,
        retention_days: int = 7,
    ) -> None:
        self._backend = backend
        self._stream_name = stream_name
        self._retention_days = retention_days

    @property
    def stream_name(self) -> str:
        """Get the DLQ stream name."""
        return self._stream_name

    async def add(
        self,
        work_item: WorkItem,
        error: Exception,
        source_stream: str | None = None,
    ) -> str:
        """
        Add a work item to the dead letter queue.

        Args:
            work_item: The work item.
            error: The error that caused the failure.
            source_stream: Original stream name.

        Returns:
            The DLQ message ID.
        """
        # Enrich metadata
        work_item.metadata.update({
            "dlq_added_at": datetime.utcnow().isoformat(),
            "dlq_error": str(error),
            "dlq_error_type": type(error).__name__,
            "dlq_source_stream": source_stream,
            "dlq_final_attempt": work_item.attempt,
        })

        message_id = await self._backend.move_to_dlq(
            work_item=work_item,
            error=error,
            dlq_stream=self._stream_name,
        )

        logger.warning(
            "Added to DLQ",
            work_item_id=work_item.id,
            agent_id=work_item.agent_id,
            document_id=work_item.document_id,
            error=str(error),
            attempts=work_item.attempt,
        )

        return message_id

    async def list(
        self,
        count: int = 100,
        start_id: str = "-",
        end_id: str = "+",
    ) -> list[dict[str, Any]]:
        """
        List items in the dead letter queue.

        Args:
            count: Maximum items to return.
            start_id: Start message ID.
            end_id: End message ID.

        Returns:
            List of DLQ items with metadata.
        """
        try:
            messages = await self._backend.client.xrange(
                self._stream_name,
                min=start_id,
                max=end_id,
                count=count,
            )

            items = []
            for message_id, data in messages:
                try:
                    work_item = deserialize_work_item(data.get("data", ""))
                    items.append({
                        "message_id": message_id,
                        "work_item_id": work_item.id,
                        "agent_id": work_item.agent_id,
                        "document_id": work_item.document_id,
                        "attempts": work_item.attempt,
                        "error": work_item.metadata.get("dlq_error"),
                        "error_type": work_item.metadata.get("dlq_error_type"),
                        "added_at": work_item.metadata.get("dlq_added_at"),
                    })
                except Exception:
                    items.append({
                        "message_id": message_id,
                        "error": "Failed to deserialize",
                    })

            return items

        except Exception as e:
            logger.error("Failed to list DLQ items", error=str(e))
            return []

    async def get(self, message_id: str) -> WorkItem | None:
        """
        Get a specific item from the DLQ.

        Args:
            message_id: The message ID.

        Returns:
            The work item or None.
        """
        try:
            messages = await self._backend.client.xrange(
                self._stream_name,
                min=message_id,
                max=message_id,
                count=1,
            )

            if messages:
                _, data = messages[0]
                return deserialize_work_item(data.get("data", ""))

            return None

        except Exception as e:
            logger.error(
                "Failed to get DLQ item",
                message_id=message_id,
                error=str(e),
            )
            return None

    async def retry(
        self,
        message_id: str,
        target_stream: str,
    ) -> str | None:
        """
        Retry a DLQ item by moving it back to a work stream.

        Args:
            message_id: The DLQ message ID.
            target_stream: The stream to move to.

        Returns:
            New message ID or None if failed.
        """
        work_item = await self.get(message_id)
        if not work_item:
            logger.warning("DLQ item not found for retry", message_id=message_id)
            return None

        # Reset attempt counter
        work_item.attempt = 0
        work_item.metadata["dlq_retried_at"] = datetime.utcnow().isoformat()

        # Enqueue to target stream
        new_message_id = await self._backend.enqueue(work_item, target_stream)

        # Remove from DLQ
        await self.delete(message_id)

        logger.info(
            "Retried DLQ item",
            work_item_id=work_item.id,
            target_stream=target_stream,
            new_message_id=new_message_id,
        )

        return new_message_id

    async def delete(self, message_id: str) -> bool:
        """
        Delete an item from the DLQ.

        Args:
            message_id: The message ID.

        Returns:
            True if deleted.
        """
        try:
            result = await self._backend.client.xdel(self._stream_name, message_id)
            return result > 0
        except Exception as e:
            logger.error(
                "Failed to delete DLQ item",
                message_id=message_id,
                error=str(e),
            )
            return False

    async def purge(self, older_than_days: int | None = None) -> int:
        """
        Purge old items from the DLQ.

        Args:
            older_than_days: Delete items older than this (uses retention_days if None).

        Returns:
            Number of items deleted.
        """
        days = older_than_days or self._retention_days
        cutoff = datetime.utcnow() - timedelta(days=days)
        cutoff_ms = int(cutoff.timestamp() * 1000)

        # Redis stream IDs are timestamp-based
        # Format: <millisecondsTime>-<sequenceNumber>
        try:
            deleted = await self._backend.client.xtrim(
                self._stream_name,
                minid=f"{cutoff_ms}-0",
            )

            if deleted:
                logger.info(
                    "Purged DLQ items",
                    count=deleted,
                    older_than_days=days,
                )

            return deleted

        except Exception as e:
            logger.error("Failed to purge DLQ", error=str(e))
            return 0

    async def count(self) -> int:
        """Get the number of items in the DLQ."""
        return await self._backend.get_stream_length(self._stream_name)

    async def get_stats(self) -> dict[str, Any]:
        """Get DLQ statistics."""
        count = await self.count()

        return {
            "stream_name": self._stream_name,
            "count": count,
            "retention_days": self._retention_days,
        }
