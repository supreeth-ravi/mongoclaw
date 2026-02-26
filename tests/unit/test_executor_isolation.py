"""Unit tests for executor failure isolation controls."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from mongoclaw.core.config import Settings
from mongoclaw.worker import executor as executor_module
from mongoclaw.worker.executor import Executor


@pytest.mark.asyncio
async def test_agent_enters_and_exits_quarantine_after_failure_budget() -> None:
    settings = Settings()
    settings.worker.agent_error_budget_max_failures = 2
    settings.worker.agent_error_budget_window_seconds = 5.0
    settings.worker.agent_quarantine_seconds = 0.05

    executor = Executor(agent_store=MagicMock(), settings=settings)
    executor_module._agent_failure_events.clear()  # noqa: SLF001
    executor_module._agent_quarantine_until.clear()  # noqa: SLF001

    await executor._record_agent_outcome("a1", False)  # noqa: SLF001
    assert await executor._is_agent_quarantined("a1") is False  # noqa: SLF001

    await executor._record_agent_outcome("a1", False)  # noqa: SLF001
    assert await executor._is_agent_quarantined("a1") is True  # noqa: SLF001

    await asyncio.sleep(0.06)
    assert await executor._is_agent_quarantined("a1") is False  # noqa: SLF001


@pytest.mark.asyncio
async def test_success_outcome_does_not_trigger_quarantine() -> None:
    settings = Settings()
    settings.worker.agent_error_budget_max_failures = 1
    executor = Executor(agent_store=MagicMock(), settings=settings)
    executor_module._agent_failure_events.clear()  # noqa: SLF001
    executor_module._agent_quarantine_until.clear()  # noqa: SLF001

    await executor._record_agent_outcome("a2", True)  # noqa: SLF001
    assert await executor._is_agent_quarantined("a2") is False  # noqa: SLF001
