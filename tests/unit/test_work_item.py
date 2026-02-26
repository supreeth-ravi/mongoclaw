"""Unit tests for work item version extraction."""

from __future__ import annotations

from mongoclaw.core.types import ChangeEvent, ChangeOperation
from mongoclaw.dispatcher.work_item import WorkItem


def test_from_event_sets_source_version_when_present() -> None:
    event = ChangeEvent(
        operation=ChangeOperation.INSERT,
        database="db",
        collection="items",
        document_key={"_id": "doc-1"},
        full_document={"_id": "doc-1", "_mongoclaw_version": 3},
    )

    work_item = WorkItem.from_event(agent_id="a1", event=event)
    assert work_item.source_version == 3


def test_from_event_defaults_source_version_to_zero_when_missing() -> None:
    event = ChangeEvent(
        operation=ChangeOperation.INSERT,
        database="db",
        collection="items",
        document_key={"_id": "doc-2"},
        full_document={"_id": "doc-2"},
    )

    work_item = WorkItem.from_event(agent_id="a1", event=event)
    assert work_item.source_version == 0


def test_source_document_hash_ignores_framework_metadata() -> None:
    event_a = ChangeEvent(
        operation=ChangeOperation.INSERT,
        database="db",
        collection="items",
        document_key={"_id": "doc-3"},
        full_document={
            "_id": "doc-3",
            "text": "hello",
            "_ai_metadata": {"source_agent_id": "x"},
            "_mongoclaw_version": 2,
        },
    )
    event_b = ChangeEvent(
        operation=ChangeOperation.INSERT,
        database="db",
        collection="items",
        document_key={"_id": "doc-3"},
        full_document={"_id": "doc-3", "text": "hello"},
    )

    a = WorkItem.from_event(agent_id="a1", event=event_a)
    b = WorkItem.from_event(agent_id="a1", event=event_b)
    assert a.source_document_hash == b.source_document_hash
