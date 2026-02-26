"""Unit tests for result writer behavior."""

from __future__ import annotations

from unittest.mock import MagicMock

from mongoclaw.agents.models import WriteConfig
from mongoclaw.core.types import AIResponse
from mongoclaw.result.writer import ResultWriter


def test_build_update_with_target_field_nests_content() -> None:
    """`target_field` should nest AI output under that field."""
    writer = ResultWriter(client=MagicMock())
    config = WriteConfig(strategy="merge", target_field="ai_classification")
    response = AIResponse(
        content='{"category":"technical","priority":"high"}',
        model="gpt-4o-mini",
        provider="openai",
    )

    update = writer._build_update(  # noqa: SLF001 - testing internal mapping
        write_config=config,
        parsed_content={"category": "technical", "priority": "high"},
        ai_response=response,
        work_item_id="wid-1",
    )

    assert update["$set"]["ai_classification"] == {
        "category": "technical",
        "priority": "high",
    }
    assert "_ai_metadata" in update["$set"]


def test_build_update_strict_mode_increments_version() -> None:
    """Strict mode should bump `_mongoclaw_version` atomically."""
    writer = ResultWriter(client=MagicMock())
    config = WriteConfig(strategy="merge")
    response = AIResponse(content='{"ok":true}', model="gpt-4o-mini", provider="openai")

    update = writer._build_update(  # noqa: SLF001
        write_config=config,
        parsed_content={"ok": True},
        ai_response=response,
        work_item_id="wid-2",
        increment_version=True,
    )

    assert update["$inc"]["_mongoclaw_version"] == 1


def test_build_update_filter_strict_default_zero_allows_missing_or_zero() -> None:
    """Strict mode with unknown version should match zero or missing stamp."""
    writer = ResultWriter(client=MagicMock())

    filter_doc = writer._build_update_filter(  # noqa: SLF001
        document_id="abc123",
        source_version=None,
        enforce_strict_version=True,
    )

    assert filter_doc["_id"] == "abc123"
    assert {"_mongoclaw_version": 0} in filter_doc["$or"]
    assert {"_mongoclaw_version": {"$exists": False}} in filter_doc["$or"]


def test_build_update_filter_strict_uses_explicit_version() -> None:
    """Strict mode should require the observed source version when provided."""
    writer = ResultWriter(client=MagicMock())

    filter_doc = writer._build_update_filter(  # noqa: SLF001
        document_id="abc123",
        source_version=7,
        enforce_strict_version=True,
    )

    assert filter_doc["_id"] == "abc123"
    assert filter_doc["_mongoclaw_version"] == 7


def test_stable_document_hash_ignores_framework_metadata() -> None:
    writer = ResultWriter(client=MagicMock())
    doc_a = {
        "_id": "d1",
        "text": "hello",
        "_ai_metadata": {"source_agent_id": "a"},
        "_mongoclaw_version": 5,
    }
    doc_b = {"_id": "d1", "text": "hello"}

    hash_a = writer._stable_document_hash(doc_a)  # noqa: SLF001
    hash_b = writer._stable_document_hash(doc_b)  # noqa: SLF001
    assert hash_a == hash_b
