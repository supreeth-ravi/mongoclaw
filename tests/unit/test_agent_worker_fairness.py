"""Unit tests for fair worker scheduling behavior."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from mongoclaw.core.config import Settings
from mongoclaw.worker import agent_worker as worker_module
from mongoclaw.worker.agent_worker import AgentWorker


def _worker(settings: Settings, streams: list[str]) -> AgentWorker:
    return AgentWorker(
        worker_id="w1",
        queue_backend=MagicMock(),
        agent_store=MagicMock(),
        settings=settings,
        streams=streams,
        executor=MagicMock(),
    )


def test_stream_order_rotates_when_fair_enabled() -> None:
    settings = Settings()
    settings.worker.fair_scheduling_enabled = True

    worker = _worker(settings, ["s1", "s2", "s3"])
    assert worker._next_stream_order() == ["s1", "s2", "s3"]  # noqa: SLF001
    assert worker._next_stream_order() == ["s2", "s3", "s1"]  # noqa: SLF001
    assert worker._next_stream_order() == ["s3", "s1", "s2"]  # noqa: SLF001


def test_stream_order_stays_stable_when_fair_disabled() -> None:
    settings = Settings()
    settings.worker.fair_scheduling_enabled = False

    worker = _worker(settings, ["s1", "s2"])
    assert worker._next_stream_order() == ["s1", "s2"]  # noqa: SLF001
    assert worker._next_stream_order() == ["s1", "s2"]  # noqa: SLF001


def test_dequeue_count_uses_fair_batch_size() -> None:
    settings = Settings()
    settings.worker.fair_scheduling_enabled = True
    settings.worker.fair_stream_batch_size = 2

    worker = _worker(settings, ["s1", "s2"])
    assert worker._dequeue_count_for_cycle(10) == 2  # noqa: SLF001


def test_stream_limit_respects_configured_cycle_cap() -> None:
    settings = Settings()
    settings.worker.fair_scheduling_enabled = True
    settings.worker.fair_streams_per_cycle = 2

    worker = _worker(settings, ["s1", "s2", "s3", "s4"])
    assert worker._stream_limit_for_cycle(4) == 2  # noqa: SLF001


def test_agent_id_from_stream_parses_agent_stream() -> None:
    settings = Settings()
    worker = _worker(settings, ["mongoclaw:agent:a1"])
    assert worker._agent_id_from_stream("mongoclaw:agent:a1") == "a1"  # noqa: SLF001
    assert worker._agent_id_from_stream("mongoclaw:priority:1") is None  # noqa: SLF001


def test_record_empty_cycle_emits_at_threshold() -> None:
    settings = Settings()
    settings.worker.starvation_cycle_threshold = 2
    worker = _worker(settings, ["mongoclaw:agent:a1"])

    worker._record_empty_cycle("mongoclaw:agent:a1")  # noqa: SLF001
    assert worker._empty_cycles_by_stream["mongoclaw:agent:a1"] == 1  # noqa: SLF001
    worker._record_empty_cycle("mongoclaw:agent:a1")  # noqa: SLF001
    assert worker._empty_cycles_by_stream["mongoclaw:agent:a1"] == 2  # noqa: SLF001


@pytest.mark.asyncio
async def test_is_stream_saturated_respects_inflight_cap() -> None:
    settings = Settings()
    settings.worker.max_in_flight_per_agent_stream = 1
    worker = _worker(settings, ["mongoclaw:agent:a1"])

    worker_module._stream_inflight_counts["mongoclaw:agent:a1"] = 1  # noqa: SLF001
    try:
        saturated = await worker._is_stream_saturated("mongoclaw:agent:a1")  # noqa: SLF001
        assert saturated is True
    finally:
        worker_module._stream_inflight_counts.clear()  # noqa: SLF001
