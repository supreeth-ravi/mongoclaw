"""Individual worker for processing work items."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

from mongoclaw.core.config import Settings
from mongoclaw.core.exceptions import (
    AgentDisabledError,
    AgentNotFoundError,
    ExecutionTimeoutError,
)
from mongoclaw.dispatcher.work_item import WorkItem, WorkItemResult
from mongoclaw.observability.logging import get_logger
from mongoclaw.worker.executor import Executor

if TYPE_CHECKING:
    from mongoclaw.agents.store import AgentStore
    from mongoclaw.queue.redis_stream import RedisStreamBackend

logger = get_logger(__name__)


class AgentWorker:
    """
    Individual worker that processes work items from queues.

    Features:
    - Concurrent processing from multiple streams
    - Automatic retry on failure
    - Graceful shutdown
    - Per-worker statistics
    """

    def __init__(
        self,
        worker_id: str,
        queue_backend: RedisStreamBackend,
        agent_store: AgentStore,
        settings: Settings,
        streams: list[str] | None = None,
        executor: Executor | None = None,
    ) -> None:
        self._worker_id = worker_id
        self._queue = queue_backend
        self._agent_store = agent_store
        self._settings = settings
        self._streams = streams or []

        self._executor = executor or Executor(
            agent_store=agent_store,
            settings=settings,
        )

        self._running = False
        self._current_item: WorkItem | None = None

        # Stats
        self._processed_count = 0
        self._error_count = 0
        self._last_process_time: float | None = None

    @property
    def worker_id(self) -> str:
        """Get the worker identifier."""
        return self._worker_id

    @property
    def is_running(self) -> bool:
        """Check if worker is running."""
        return self._running

    def update_streams(self, streams: list[str]) -> None:
        """Update the list of streams to consume."""
        self._streams = streams

    async def run(self, shutdown_event: asyncio.Event) -> None:
        """
        Run the worker processing loop.

        Args:
            shutdown_event: Event to signal shutdown.
        """
        self._running = True
        consumer_group = self._settings.redis.consumer_group
        batch_size = self._settings.worker.batch_size
        block_ms = self._settings.redis.block_ms

        logger.info(
            "Worker started",
            worker_id=self._worker_id,
            streams=len(self._streams),
        )

        while self._running and not shutdown_event.is_set():
            try:
                # Process from each stream
                for stream in self._streams:
                    if shutdown_event.is_set():
                        break

                    items = await self._queue.dequeue(
                        stream_name=stream,
                        consumer_group=consumer_group,
                        consumer_name=self._worker_id,
                        count=batch_size,
                        block_ms=block_ms,
                    )

                    for message_id, work_item in items:
                        if shutdown_event.is_set():
                            break

                        await self._process_item(
                            stream,
                            message_id,
                            work_item,
                        )

            except asyncio.CancelledError:
                break

            except Exception as e:
                logger.exception(
                    "Worker error",
                    worker_id=self._worker_id,
                    error=str(e),
                )
                await asyncio.sleep(1)  # Brief pause on error

        self._running = False
        logger.info(
            "Worker stopped",
            worker_id=self._worker_id,
            processed=self._processed_count,
            errors=self._error_count,
        )

    async def stop(self) -> None:
        """Stop the worker."""
        self._running = False

    async def _process_item(
        self,
        stream: str,
        message_id: str,
        work_item: WorkItem,
    ) -> None:
        """Process a single work item."""
        start_time = time.perf_counter()
        self._current_item = work_item

        log_context = {
            "worker_id": self._worker_id,
            "work_item_id": work_item.id,
            "agent_id": work_item.agent_id,
            "document_id": work_item.document_id,
            "attempt": work_item.attempt,
        }

        logger.debug("Processing work item", **log_context)

        try:
            # Execute the work item
            result = await self._executor.execute(work_item)

            duration_ms = (time.perf_counter() - start_time) * 1000

            if result.success:
                # Acknowledge successful processing
                await self._queue.ack(
                    stream,
                    self._settings.redis.consumer_group,
                    message_id,
                )

                self._processed_count += 1
                self._last_process_time = time.time()

                logger.info(
                    "Work item completed",
                    duration_ms=round(duration_ms, 2),
                    **log_context,
                )

            else:
                # Handle failure
                await self._handle_failure(
                    stream,
                    message_id,
                    work_item,
                    result,
                )

        except (AgentNotFoundError, AgentDisabledError) as e:
            # Don't retry for these errors
            await self._queue.ack(
                stream,
                self._settings.redis.consumer_group,
                message_id,
            )
            logger.warning(
                "Agent error, not retrying",
                error=str(e),
                **log_context,
            )

        except ExecutionTimeoutError as e:
            await self._handle_timeout(
                stream,
                message_id,
                work_item,
                e,
            )

        except Exception as e:
            logger.exception(
                "Unexpected error processing work item",
                error=str(e),
                **log_context,
            )

            # Create failure result
            duration_ms = (time.perf_counter() - start_time) * 1000
            result = WorkItemResult.failure_result(
                work_item, e, duration_ms
            )
            await self._handle_failure(
                stream,
                message_id,
                work_item,
                result,
            )

        finally:
            self._current_item = None

    async def _handle_failure(
        self,
        stream: str,
        message_id: str,
        work_item: WorkItem,
        result: WorkItemResult,
    ) -> None:
        """Handle a failed work item."""
        self._error_count += 1

        if work_item.should_retry():
            # Re-enqueue with incremented attempt
            retried_item = work_item.increment_attempt()

            # Calculate retry delay
            delay = self._calculate_retry_delay(retried_item.attempt)

            logger.info(
                "Retrying work item",
                work_item_id=work_item.id,
                attempt=retried_item.attempt,
                delay_seconds=delay,
            )

            # For simplicity, we re-enqueue immediately
            # A more sophisticated approach would use scheduled delivery
            await self._queue.enqueue(retried_item, stream)

        else:
            # Move to dead letter queue
            from mongoclaw.dispatcher.routing import get_dlq_stream_name

            dlq_stream = get_dlq_stream_name()

            error = Exception(result.error or "Unknown error")
            await self._queue.move_to_dlq(work_item, error, dlq_stream)

            logger.warning(
                "Work item moved to DLQ",
                work_item_id=work_item.id,
                error=result.error,
            )

        # Acknowledge original message
        await self._queue.ack(
            stream,
            self._settings.redis.consumer_group,
            message_id,
        )

    async def _handle_timeout(
        self,
        stream: str,
        message_id: str,
        work_item: WorkItem,
        error: ExecutionTimeoutError,
    ) -> None:
        """Handle a timed out work item."""
        self._error_count += 1

        logger.warning(
            "Work item timed out",
            work_item_id=work_item.id,
            timeout=error.details.get("timeout_seconds"),
        )

        if work_item.should_retry():
            retried_item = work_item.increment_attempt()
            await self._queue.enqueue(retried_item, stream)
        else:
            from mongoclaw.dispatcher.routing import get_dlq_stream_name

            dlq_stream = get_dlq_stream_name()
            await self._queue.move_to_dlq(work_item, error, dlq_stream)

        await self._queue.ack(
            stream,
            self._settings.redis.consumer_group,
            message_id,
        )

    def _calculate_retry_delay(self, attempt: int) -> float:
        """Calculate exponential backoff delay."""
        base = self._settings.worker.retry_base_delay
        max_delay = self._settings.worker.retry_max_delay

        delay = base * (2 ** (attempt - 1))
        return min(delay, max_delay)

    def get_stats(self) -> dict[str, Any]:
        """Get worker statistics."""
        return {
            "worker_id": self._worker_id,
            "running": self._running,
            "processed_count": self._processed_count,
            "error_count": self._error_count,
            "streams": len(self._streams),
            "current_item": self._current_item.id if self._current_item else None,
            "last_process_time": self._last_process_time,
        }
