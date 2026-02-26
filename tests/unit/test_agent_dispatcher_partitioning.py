"""Unit tests for partitioned routing and delivery semantics metadata."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from mongoclaw.agents.models import AgentConfig
from mongoclaw.core.config import Settings
from mongoclaw.core.types import ChangeEvent, ChangeOperation
from mongoclaw.dispatcher.agent_dispatcher import AgentDispatcher
from mongoclaw.dispatcher.routing import RoutingStrategy


class _CaptureQueue:
    def __init__(self) -> None:
        self.items: list[tuple[object, str]] = []

    async def get_stream_length(self, stream_name: str) -> int:
        return 0

    async def enqueue(self, work_item, stream_name: str) -> str:  # noqa: ANN001
        self.items.append((work_item, stream_name))
        return "1-0"

    async def move_to_dlq(self, work_item, error: Exception, dlq_stream: str) -> str:  # noqa: ANN001
        return "1-1"


def _agent() -> AgentConfig:
    return AgentConfig.model_validate({
        "id": "partition_agent",
        "name": "partition agent",
        "watch": {
            "database": "db",
            "collection": "c",
            "operations": ["insert"],
        },
        "ai": {"model": "gpt-4o-mini", "prompt": "x"},
        "write": {"strategy": "merge"},
        "execution": {"deduplicate": False, "max_retries": 1},
    })


def _event(document_id: str = "d1") -> ChangeEvent:
    return ChangeEvent(
        operation=ChangeOperation.INSERT,
        database="db",
        collection="c",
        document_key={"_id": document_id},
        full_document={"_id": document_id, "text": "hello"},
    )


@pytest.mark.asyncio
async def test_partitioned_routing_uses_configured_partition_count_and_metadata() -> None:
    settings = Settings()
    settings.worker.routing_partition_count = 4
    queue = _CaptureQueue()
    dispatcher = AgentDispatcher(
        agent_store=MagicMock(),
        queue_backend=queue,
        settings=settings,
        routing_strategy=RoutingStrategy.PARTITIONED,
    )

    work_item_id = await dispatcher.dispatch(_agent(), _event("doc-42"))
    assert work_item_id is not None
    assert len(queue.items) == 1

    work_item, stream = queue.items[0]
    assert stream.startswith("mongoclaw:partition:")
    partition = int(stream.rsplit(":", 1)[1])
    assert 0 <= partition < 4
    assert work_item.metadata["delivery_semantics"] == "at_least_once"
    assert work_item.metadata["routing_strategy"] == "partitioned"
    assert work_item.metadata["partition"] == partition


@pytest.mark.asyncio
async def test_by_agent_routing_sets_semantics_without_partition() -> None:
    settings = Settings()
    queue = _CaptureQueue()
    dispatcher = AgentDispatcher(
        agent_store=MagicMock(),
        queue_backend=queue,
        settings=settings,
        routing_strategy=RoutingStrategy.BY_AGENT,
    )

    work_item_id = await dispatcher.dispatch(_agent(), _event("doc-7"))
    assert work_item_id is not None
    work_item, stream = queue.items[0]
    assert stream == "mongoclaw:agent:partition_agent"
    assert work_item.metadata["delivery_semantics"] == "at_least_once"
    assert work_item.metadata["routing_strategy"] == "by_agent"
    assert "partition" not in work_item.metadata
