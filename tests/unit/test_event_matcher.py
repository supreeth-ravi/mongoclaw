"""Unit tests for event matcher loop guard behavior."""

from __future__ import annotations

import pytest

from mongoclaw.agents.models import AgentConfig
from mongoclaw.core.types import ChangeEvent, ChangeOperation
from mongoclaw.watcher.event_matcher import EventMatcher


class _FakeAgentStore:
    def __init__(self, agents: list[AgentConfig]) -> None:
        self._agents = agents

    async def get_by_watch_target(
        self,
        database: str,
        collection: str,
        enabled_only: bool = True,
    ) -> list[AgentConfig]:
        return [
            a
            for a in self._agents
            if a.watch.database == database
            and a.watch.collection == collection
            and (a.enabled if enabled_only else True)
        ]


def _agent() -> AgentConfig:
    return AgentConfig.model_validate(
        {
            "id": "loop_agent",
            "name": "Loop Agent",
            "watch": {
                "database": "loopdb",
                "collection": "items",
                "operations": ["insert", "update"],
            },
            "ai": {
                "model": "gpt-4o-mini",
                "prompt": "test {{ document.title }}",
            },
            "write": {"strategy": "merge"},
            "enabled": True,
        }
    )


@pytest.mark.asyncio
async def test_loop_guard_skips_self_mutated_document() -> None:
    agent = _agent()
    matcher = EventMatcher(_FakeAgentStore([agent]))

    event = ChangeEvent(
        operation=ChangeOperation.UPDATE,
        database="loopdb",
        collection="items",
        document_key={"_id": "doc1"},
        full_document={
            "_id": "doc1",
            "title": "x",
            "_ai_metadata": {"source_agent_id": "loop_agent"},
        },
    )

    matched = await matcher.match(event)
    assert matched == []


@pytest.mark.asyncio
async def test_loop_guard_allows_non_self_metadata() -> None:
    agent = _agent()
    matcher = EventMatcher(_FakeAgentStore([agent]))

    event = ChangeEvent(
        operation=ChangeOperation.UPDATE,
        database="loopdb",
        collection="items",
        document_key={"_id": "doc2"},
        full_document={
            "_id": "doc2",
            "title": "x",
            "_ai_metadata": {"source_agent_id": "other_agent"},
        },
    )

    matched = await matcher.match(event)
    assert len(matched) == 1
    assert matched[0].id == "loop_agent"
