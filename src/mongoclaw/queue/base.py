"""Abstract base class for queue backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from mongoclaw.dispatcher.work_item import WorkItem


class QueueBackendBase(ABC):
    """
    Abstract base class for queue backend implementations.

    Queue backends handle the storage and retrieval of work items
    for asynchronous processing by workers.
    """

    @abstractmethod
    async def connect(self) -> None:
        """
        Connect to the queue backend.

        Raises:
            QueueConnectionError: If connection fails.
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the queue backend."""
        pass

    @abstractmethod
    async def enqueue(self, work_item: WorkItem, stream_name: str) -> str:
        """
        Enqueue a work item.

        Args:
            work_item: The work item to enqueue.
            stream_name: The stream/queue name.

        Returns:
            The message ID.

        Raises:
            QueueError: If enqueue fails.
        """
        pass

    @abstractmethod
    async def dequeue(
        self,
        stream_name: str,
        consumer_group: str,
        consumer_name: str,
        count: int = 1,
        block_ms: int = 5000,
    ) -> list[tuple[str, WorkItem]]:
        """
        Dequeue work items.

        Args:
            stream_name: The stream/queue name.
            consumer_group: The consumer group name.
            consumer_name: The consumer name.
            count: Maximum number of items to dequeue.
            block_ms: Block time in milliseconds.

        Returns:
            List of (message_id, work_item) tuples.
        """
        pass

    @abstractmethod
    async def ack(
        self,
        stream_name: str,
        consumer_group: str,
        message_id: str,
    ) -> None:
        """
        Acknowledge a message.

        Args:
            stream_name: The stream/queue name.
            consumer_group: The consumer group name.
            message_id: The message ID to acknowledge.
        """
        pass

    @abstractmethod
    async def move_to_dlq(
        self,
        work_item: WorkItem,
        error: Exception,
        dlq_stream: str,
    ) -> str:
        """
        Move a work item to the dead letter queue.

        Args:
            work_item: The work item to move.
            error: The error that caused the move.
            dlq_stream: The dead letter queue stream name.

        Returns:
            The DLQ message ID.
        """
        pass

    @abstractmethod
    async def get_pending_count(
        self,
        stream_name: str,
        consumer_group: str,
    ) -> int:
        """Get the count of pending messages."""
        pass

    @abstractmethod
    async def get_stream_length(self, stream_name: str) -> int:
        """Get the length of a stream."""
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if the queue backend is healthy.

        Returns:
            True if healthy.
        """
        pass

    async def get_stats(self) -> dict[str, Any]:
        """
        Get queue statistics.

        Returns:
            Dictionary of statistics.
        """
        return {}
