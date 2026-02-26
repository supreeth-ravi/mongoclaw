"""Individual worker for processing work items."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

from motor.motor_asyncio import AsyncIOMotorClient

from mongoclaw.core.config import Settings
from mongoclaw.core.exceptions import (
    AgentDisabledError,
    AgentNotFoundError,
    ExecutionTimeoutError,
)
from mongoclaw.dispatcher.work_item import WorkItem, WorkItemResult
from mongoclaw.observability.logging import get_logger
from mongoclaw.observability.metrics import get_metrics_collector
from mongoclaw.worker.executor import Executor

if TYPE_CHECKING:
    from mongoclaw.agents.store import AgentStore
    from mongoclaw.queue.redis_stream import RedisStreamBackend

logger = get_logger(__name__)

_inflight_lock = asyncio.Lock()
_stream_inflight_counts: dict[str, int] = {}


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
        mongo_client: AsyncIOMotorClient[dict[str, Any]] | None = None,
    ) -> None:
        self._worker_id = worker_id
        self._queue = queue_backend
        self._agent_store = agent_store
        self._settings = settings
        self._streams = streams or []

        self._executor = executor or Executor(
            agent_store=agent_store,
            settings=settings,
            mongo_client=mongo_client,
        )

        self._running = False
        self._current_item: WorkItem | None = None
        self._shutdown_event: asyncio.Event | None = None
        self._stream_cursor = 0
        self._pending_sampled_at: dict[str, float] = {}
        self._empty_cycles_by_stream: dict[str, int] = {}

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
        self._shutdown_event = shutdown_event
        consumer_group = self._settings.redis.consumer_group
        batch_size = self._settings.worker.batch_size
        block_ms = self._settings.redis.block_ms

        logger.info(
            "Worker started",
            worker_id=self._worker_id,
            streams=len(self._streams),
        )

        while self._running and not shutdown_event.is_set():
            ordered_streams = self._next_stream_order()
            stream_count = max(1, len(ordered_streams))
            effective_block_ms = max(100, block_ms // stream_count)
            # Process from each stream - wrap each in try/except to prevent starvation
            dequeue_count = self._dequeue_count_for_cycle(batch_size)
            stream_limit = self._stream_limit_for_cycle(len(ordered_streams))
            for idx, stream in enumerate(ordered_streams):
                if idx >= stream_limit:
                    break
                if shutdown_event.is_set():
                    break

                try:
                    await self._sample_stream_pending_if_due(stream, consumer_group)
                    if await self._is_stream_saturated(stream):
                        continue

                    items = await self._queue.dequeue(
                        stream_name=stream,
                        consumer_group=consumer_group,
                        consumer_name=self._worker_id,
                        count=dequeue_count,
                        block_ms=effective_block_ms,
                    )
                    if not items:
                        self._record_empty_cycle(stream)
                        continue
                    self._empty_cycles_by_stream[stream] = 0

                    for message_id, work_item in items:
                        if shutdown_event.is_set():
                            break

                        await self._increment_stream_inflight(stream)
                        try:
                            await self._process_item(
                                stream,
                                message_id,
                                work_item,
                            )
                        finally:
                            await self._decrement_stream_inflight(stream)

                except asyncio.CancelledError:
                    self._running = False
                    break

                except Exception as e:
                    # Log error but continue to next stream to prevent starvation
                    logger.warning(
                        "Error processing stream",
                        worker_id=self._worker_id,
                        stream=stream,
                        error=str(e),
                    )
                    # Brief pause before trying next stream
                    await asyncio.sleep(0.1)

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
        if work_item.attempt > 0:
            get_metrics_collector().record_replayed_delivery(work_item.agent_id)

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
            # Delay retries to avoid retry storms under provider pressure.
            await self._sleep_with_shutdown(delay)
            await self._queue.enqueue(retried_item, stream)
            get_metrics_collector().record_retry_scheduled(
                work_item.agent_id,
                "failure",
            )

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
            delay = self._calculate_retry_delay(retried_item.attempt)
            await self._sleep_with_shutdown(delay)
            await self._queue.enqueue(retried_item, stream)
            get_metrics_collector().record_retry_scheduled(
                work_item.agent_id,
                "timeout",
            )
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

    async def _sleep_with_shutdown(self, delay_seconds: float) -> None:
        """Sleep for retry delay, but exit early if shutdown begins."""
        if delay_seconds <= 0:
            return
        if self._shutdown_event is None:
            await asyncio.sleep(delay_seconds)
            return
        try:
            await asyncio.wait_for(self._shutdown_event.wait(), timeout=delay_seconds)
        except asyncio.TimeoutError:
            return

    def _next_stream_order(self) -> list[str]:
        """Return streams in rotated order for fair scheduling."""
        if not self._streams:
            return []

        if (
            not self._settings.worker.fair_scheduling_enabled
            or len(self._streams) == 1
        ):
            return list(self._streams)

        cursor = self._stream_cursor % len(self._streams)
        ordered = self._streams[cursor:] + self._streams[:cursor]
        self._stream_cursor = (cursor + 1) % len(self._streams)
        return ordered

    def _dequeue_count_for_cycle(self, batch_size: int) -> int:
        """Determine dequeue count for each stream in a cycle."""
        if not self._settings.worker.fair_scheduling_enabled:
            return batch_size
        return max(
            1,
            min(batch_size, self._settings.worker.fair_stream_batch_size),
        )

    def _stream_limit_for_cycle(self, stream_count: int) -> int:
        """Limit number of streams processed in one cycle if configured."""
        limit = self._settings.worker.fair_streams_per_cycle
        if limit is None:
            return stream_count
        return max(1, min(stream_count, limit))

    async def _is_stream_saturated(self, stream: str) -> bool:
        """Return True when stream hit the configured in-flight cap."""
        cap = self._settings.worker.max_in_flight_per_agent_stream
        if cap is None:
            return False

        agent_id = self._agent_id_from_stream(stream)
        if not agent_id:
            return False

        async with _inflight_lock:
            current = _stream_inflight_counts.get(stream, 0)
        if current < cap:
            return False

        get_metrics_collector().record_agent_stream_saturation_skip(agent_id, stream)
        logger.debug(
            "Skipping saturated stream",
            worker_id=self._worker_id,
            stream=stream,
            in_flight=current,
            cap=cap,
        )
        return True

    async def _increment_stream_inflight(self, stream: str) -> None:
        """Increment in-flight counter for a stream and publish metric."""
        agent_id = self._agent_id_from_stream(stream)
        if not agent_id:
            return
        async with _inflight_lock:
            current = _stream_inflight_counts.get(stream, 0) + 1
            _stream_inflight_counts[stream] = current
        get_metrics_collector().set_agent_stream_inflight(agent_id, stream, current)

    async def _decrement_stream_inflight(self, stream: str) -> None:
        """Decrement in-flight counter for a stream and publish metric."""
        agent_id = self._agent_id_from_stream(stream)
        if not agent_id:
            return
        async with _inflight_lock:
            current = max(0, _stream_inflight_counts.get(stream, 0) - 1)
            if current == 0:
                _stream_inflight_counts.pop(stream, None)
            else:
                _stream_inflight_counts[stream] = current
        get_metrics_collector().set_agent_stream_inflight(agent_id, stream, current)

    async def _sample_stream_pending_if_due(
        self,
        stream: str,
        consumer_group: str,
    ) -> None:
        """Sample pending queue depth for agent streams on interval."""
        agent_id = self._agent_id_from_stream(stream)
        if not agent_id:
            return

        interval = self._settings.worker.pending_metrics_interval_seconds
        now = time.monotonic()
        last = self._pending_sampled_at.get(stream)
        if last is not None and (now - last) < interval:
            return

        self._pending_sampled_at[stream] = now
        try:
            pending = await self._queue.get_pending_count(stream, consumer_group)
            get_metrics_collector().set_agent_stream_pending(agent_id, stream, pending)
        except Exception:
            logger.debug(
                "Failed pending sample",
                worker_id=self._worker_id,
                stream=stream,
            )

    def _record_empty_cycle(self, stream: str) -> None:
        """Record consecutive empty cycles and emit starvation signals."""
        agent_id = self._agent_id_from_stream(stream)
        if not agent_id:
            return

        cycles = self._empty_cycles_by_stream.get(stream, 0) + 1
        self._empty_cycles_by_stream[stream] = cycles
        threshold = self._settings.worker.starvation_cycle_threshold
        if cycles >= threshold and cycles % threshold == 0:
            get_metrics_collector().record_agent_stream_starvation_cycle(
                agent_id,
                stream,
            )

    def _agent_id_from_stream(self, stream: str) -> str | None:
        """Extract agent_id from `mongoclaw:agent:<id>` stream names."""
        prefix = "mongoclaw:agent:"
        if not stream.startswith(prefix):
            return None
        agent_id = stream[len(prefix):]
        return agent_id or None

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
