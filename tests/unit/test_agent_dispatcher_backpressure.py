"""Unit tests for dispatch-time backpressure admission policies."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from mongoclaw.agents.models import AgentConfig
from mongoclaw.core.config import Settings
from mongoclaw.core.types import ChangeEvent, ChangeOperation
from mongoclaw.dispatcher.agent_dispatcher import AgentDispatcher


class _FakeQueue:
    def __init__(self, lengths: list[int]) -> None:
        self._lengths = lengths
        self.enqueued: list[tuple[str, str]] = []
        self.dlqed: list[str] = []

    async def get_stream_length(self, stream_name: str) -> int:
        if self._lengths:
            return self._lengths.pop(0)
        return 0

    async def enqueue(self, work_item, stream_name: str) -> str:  # noqa: ANN001
        self.enqueued.append((work_item.id, stream_name))
        return "1-0"

    async def move_to_dlq(self, work_item, error: Exception, dlq_stream: str) -> str:  # noqa: ANN001
        self.dlqed.append(dlq_stream)
        return "1-1"


def _agent(priority: int = 1) -> AgentConfig:
    return AgentConfig.model_validate({
        "id": f"agent_p{priority}",
        "name": "bp agent",
        "watch": {
            "database": "db",
            "collection": "c",
            "operations": ["insert"],
        },
        "ai": {"model": "gpt-4o-mini", "prompt": "x"},
        "write": {"strategy": "merge"},
        "execution": {
            "priority": priority,
            "deduplicate": False,
            "max_retries": 1,
        },
    })


def _event() -> ChangeEvent:
    return ChangeEvent(
        operation=ChangeOperation.INSERT,
        database="db",
        collection="c",
        document_key={"_id": "d1"},
        full_document={"_id": "d1", "text": "hello"},
    )


@pytest.mark.asyncio
async def test_drop_policy_rejects_low_priority_under_pressure() -> None:
    settings = Settings()
    settings.redis.stream_max_len = 100
    settings.worker.dispatch_backpressure_threshold = 0.8
    settings.worker.dispatch_overflow_policy = "drop"

    queue = _FakeQueue(lengths=[90])  # 90%
    dispatcher = AgentDispatcher(MagicMock(), queue, settings)

    work_item_id = await dispatcher.dispatch(_agent(priority=1), _event())
    assert work_item_id is None
    assert queue.enqueued == []


@pytest.mark.asyncio
async def test_dlq_policy_routes_low_priority_to_dlq_under_pressure() -> None:
    settings = Settings()
    settings.redis.stream_max_len = 100
    settings.worker.dispatch_backpressure_threshold = 0.8
    settings.worker.dispatch_overflow_policy = "dlq"

    queue = _FakeQueue(lengths=[95])  # 95%
    dispatcher = AgentDispatcher(MagicMock(), queue, settings)

    work_item_id = await dispatcher.dispatch(_agent(priority=1), _event())
    assert work_item_id is None
    assert queue.enqueued == []
    assert queue.dlqed


@pytest.mark.asyncio
async def test_priority_bypass_allows_high_priority_under_pressure() -> None:
    settings = Settings()
    settings.redis.stream_max_len = 100
    settings.worker.dispatch_backpressure_threshold = 0.8
    settings.worker.dispatch_overflow_policy = "drop"
    settings.worker.dispatch_min_priority_when_backpressured = 5

    queue = _FakeQueue(lengths=[95])  # 95%
    dispatcher = AgentDispatcher(MagicMock(), queue, settings)

    work_item_id = await dispatcher.dispatch(_agent(priority=8), _event())
    assert work_item_id is not None
    assert len(queue.enqueued) == 1


@pytest.mark.asyncio
async def test_defer_policy_waits_then_enqueues_when_pressure_recovers() -> None:
    settings = Settings()
    settings.redis.stream_max_len = 100
    settings.worker.dispatch_backpressure_threshold = 0.8
    settings.worker.dispatch_overflow_policy = "defer"
    settings.worker.dispatch_defer_seconds = 0.001
    settings.worker.dispatch_defer_max_attempts = 2
    settings.worker.dispatch_pressure_cache_ttl_seconds = 0.000000001

    queue = _FakeQueue(lengths=[90, 60])  # high then recovered
    dispatcher = AgentDispatcher(MagicMock(), queue, settings)

    work_item_id = await dispatcher.dispatch(_agent(priority=1), _event())
    assert work_item_id is not None
    assert len(queue.enqueued) == 1
