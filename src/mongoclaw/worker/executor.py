"""Execution pipeline for processing work items."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

from motor.motor_asyncio import AsyncIOMotorClient

from mongoclaw.ai.prompt_engine import PromptEngine
from mongoclaw.ai.provider_router import ProviderRouter
from mongoclaw.ai.response_parser import ResponseParser
from mongoclaw.core.config import Settings, get_settings
from mongoclaw.core.exceptions import (
    AgentDisabledError,
    AgentNotFoundError,
    ExecutionTimeoutError,
)
from mongoclaw.core.types import AIResponse
from mongoclaw.dispatcher.work_item import WorkItem, WorkItemResult
from mongoclaw.observability.logging import get_logger
from mongoclaw.result.writer import ResultWriter

if TYPE_CHECKING:
    from mongoclaw.agents.models import AgentConfig
    from mongoclaw.agents.store import AgentStore

logger = get_logger(__name__)


class Executor:
    """
    Executes the AI enrichment pipeline for work items.

    Pipeline stages:
    1. Load agent configuration
    2. Render prompt template
    3. Call AI provider
    4. Parse response
    5. Write result to MongoDB
    """

    def __init__(
        self,
        agent_store: AgentStore,
        settings: Settings | None = None,
        mongo_client: AsyncIOMotorClient[dict[str, Any]] | None = None,
        provider_router: ProviderRouter | None = None,
        prompt_engine: PromptEngine | None = None,
        response_parser: ResponseParser | None = None,
        result_writer: ResultWriter | None = None,
    ) -> None:
        self._agent_store = agent_store
        self._settings = settings or get_settings()
        self._mongo_client = mongo_client

        # Components (lazy initialized)
        self._provider_router = provider_router
        self._prompt_engine = prompt_engine or PromptEngine()
        self._response_parser = response_parser or ResponseParser()
        self._result_writer = result_writer

        # Cache for agent configs
        self._agent_cache: dict[str, AgentConfig] = {}

    async def execute(self, work_item: WorkItem) -> WorkItemResult:
        """
        Execute the full enrichment pipeline for a work item.

        Args:
            work_item: The work item to process.

        Returns:
            WorkItemResult with success/failure details.
        """
        start_time = time.perf_counter()

        try:
            # 1. Load agent configuration
            agent = await self._get_agent(work_item.agent_id)

            if not agent.enabled:
                raise AgentDisabledError(agent.id)

            # 2. Apply timeout
            timeout = agent.execution.timeout_seconds

            result = await asyncio.wait_for(
                self._execute_pipeline(work_item, agent),
                timeout=timeout,
            )

            return result

        except asyncio.TimeoutError:
            duration_ms = (time.perf_counter() - start_time) * 1000
            agent = self._agent_cache.get(work_item.agent_id)
            timeout = agent.execution.timeout_seconds if agent else 60.0

            raise ExecutionTimeoutError(
                agent_id=work_item.agent_id,
                work_item_id=work_item.id,
                timeout_seconds=timeout,
            )

        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            return WorkItemResult.failure_result(work_item, e, duration_ms)

    async def _execute_pipeline(
        self,
        work_item: WorkItem,
        agent: AgentConfig,
    ) -> WorkItemResult:
        """Execute the pipeline stages."""
        start_time = time.perf_counter()

        # 2. Render prompt
        prompt = self._render_prompt(agent, work_item)
        system_prompt = self._render_system_prompt(agent, work_item)

        logger.debug(
            "Rendered prompt",
            agent_id=agent.id,
            prompt_length=len(prompt),
        )

        # 3. Call AI provider
        ai_response = await self._call_ai(agent, prompt, system_prompt)

        # 4. Parse response
        parsed = self._parse_response(agent, ai_response)

        # 5. Write result
        written = await self._write_result(agent, work_item, ai_response)

        duration_ms = (time.perf_counter() - start_time) * 1000

        return WorkItemResult.success_result(
            work_item=work_item,
            ai_response=ai_response.to_dict(),
            written=written,
            duration_ms=duration_ms,
        )

    async def _get_agent(self, agent_id: str) -> AgentConfig:
        """Get agent configuration, using cache."""
        if agent_id in self._agent_cache:
            return self._agent_cache[agent_id]

        agent = await self._agent_store.get_optional(agent_id)
        if agent is None:
            raise AgentNotFoundError(agent_id)

        self._agent_cache[agent_id] = agent
        return agent

    def _render_prompt(
        self,
        agent: AgentConfig,
        work_item: WorkItem,
    ) -> str:
        """Render the prompt template."""
        context = self._prompt_engine.build_context(
            document=work_item.document,
            change_event=work_item.change_event,
            agent_config=agent.model_dump(),
        )

        return self._prompt_engine.render(
            template=agent.ai.prompt,
            context=context,
            template_name=f"{agent.id}:prompt",
        )

    def _render_system_prompt(
        self,
        agent: AgentConfig,
        work_item: WorkItem,
    ) -> str | None:
        """Render the system prompt template if present."""
        if not agent.ai.system_prompt:
            return None

        context = self._prompt_engine.build_context(
            document=work_item.document,
            change_event=work_item.change_event,
            agent_config=agent.model_dump(),
        )

        return self._prompt_engine.render(
            template=agent.ai.system_prompt,
            context=context,
            template_name=f"{agent.id}:system_prompt",
        )

    async def _call_ai(
        self,
        agent: AgentConfig,
        prompt: str,
        system_prompt: str | None,
    ) -> AIResponse:
        """Call the AI provider."""
        if self._provider_router is None:
            self._provider_router = ProviderRouter(self._settings)

        ai_config = agent.ai

        # Prepare response format
        response_format = None
        if ai_config.response_schema:
            response_format = "json_object"

        response = await self._provider_router.complete(
            model=ai_config.model,
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=ai_config.temperature,
            max_tokens=ai_config.max_tokens,
            response_format=response_format,
            **ai_config.extra_params,
        )

        return response

    def _parse_response(
        self,
        agent: AgentConfig,
        ai_response: AIResponse,
    ) -> dict[str, Any]:
        """Parse the AI response."""
        schema = agent.ai.response_schema

        try:
            return self._response_parser.parse(ai_response, schema)
        except Exception as e:
            logger.warning(
                "Response parsing failed",
                agent_id=agent.id,
                error=str(e),
            )
            # Return raw content as fallback
            return {"content": ai_response.content, "_raw": True}

    async def _write_result(
        self,
        agent: AgentConfig,
        work_item: WorkItem,
        ai_response: AIResponse,
    ) -> bool:
        """Write the result to MongoDB."""
        if self._result_writer is None:
            if self._mongo_client is None:
                logger.warning(
                    "No MongoDB client, skipping write",
                    agent_id=agent.id,
                )
                return False

            self._result_writer = ResultWriter(self._mongo_client)
            await self._result_writer.initialize()

        try:
            return await self._result_writer.write(
                agent=agent,
                document_id=work_item.document_id,
                ai_response=ai_response,
                work_item_id=work_item.id,
                idempotency_key=work_item.idempotency_key,
            )
        except Exception as e:
            logger.error(
                "Failed to write result",
                agent_id=agent.id,
                document_id=work_item.document_id,
                error=str(e),
            )
            # Don't fail the whole execution for write errors
            return False

    def invalidate_agent_cache(self, agent_id: str | None = None) -> None:
        """Invalidate the agent configuration cache."""
        if agent_id:
            self._agent_cache.pop(agent_id, None)
        else:
            self._agent_cache.clear()

    def set_mongo_client(self, client: AsyncIOMotorClient[dict[str, Any]]) -> None:
        """Set the MongoDB client for result writing."""
        self._mongo_client = client
        self._result_writer = None  # Will be recreated on next use
