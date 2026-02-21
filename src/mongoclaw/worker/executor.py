"""Execution pipeline for processing work items."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from collections import deque
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
from mongoclaw.observability.metrics import get_metrics_collector
from mongoclaw.policy.evaluator import PolicyEvaluationError, evaluate_policy_condition
from mongoclaw.result.writer import ResultWriter

if TYPE_CHECKING:
    from mongoclaw.agents.models import AgentConfig
    from mongoclaw.agents.store import AgentStore

logger = get_logger(__name__)

_semaphore_lock = asyncio.Lock()
_agent_semaphores: dict[str, tuple[int, asyncio.Semaphore]] = {}
_agent_budget_lock = asyncio.Lock()
_agent_failure_events: dict[str, deque[float]] = {}
_agent_quarantine_until: dict[str, float] = {}


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
        started_at = datetime.utcnow()

        try:
            # 1. Load agent configuration
            agent = await self._get_agent(work_item.agent_id)

            if not agent.enabled:
                raise AgentDisabledError(agent.id)

            if await self._is_agent_quarantined(agent.id):
                duration_ms = (time.perf_counter() - start_time) * 1000
                result = WorkItemResult.failure_result(
                    work_item=work_item,
                    error=RuntimeError("Agent temporarily quarantined"),
                    duration_ms=duration_ms,
                    lifecycle_state="failed",
                    reason="agent_quarantined",
                )
                await self._record_execution(work_item, result, started_at)
                return result

            # 2. Apply timeout
            timeout = agent.execution.timeout_seconds

            semaphore = await self._get_agent_semaphore(agent.id, agent.execution.max_concurrency)
            if semaphore is None:
                result = await asyncio.wait_for(
                    self._execute_pipeline(work_item, agent),
                    timeout=timeout,
                )
            else:
                if semaphore.locked():
                    get_metrics_collector().record_agent_concurrency_wait(agent.id)
                async with semaphore:
                    result = await asyncio.wait_for(
                        self._execute_pipeline(work_item, agent),
                        timeout=timeout,
                    )

            await self._record_execution(work_item, result, started_at)
            await self._record_agent_outcome(agent.id, result.success)
            self._record_latency_slo_if_needed(agent.id, result.duration_ms)
            return result

        except asyncio.TimeoutError:
            duration_ms = (time.perf_counter() - start_time) * 1000
            agent = self._agent_cache.get(work_item.agent_id)
            timeout = agent.execution.timeout_seconds if agent else 60.0
            result = WorkItemResult.failure_result(
                work_item=work_item,
                error=TimeoutError(f"Execution timed out after {timeout}s"),
                duration_ms=duration_ms,
                lifecycle_state="timed_out",
                reason="timeout",
            )
            await self._record_execution(work_item, result, started_at)
            await self._record_agent_outcome(work_item.agent_id, False)
            self._record_latency_slo_if_needed(work_item.agent_id, duration_ms)

            raise ExecutionTimeoutError(
                agent_id=work_item.agent_id,
                work_item_id=work_item.id,
                timeout_seconds=timeout,
            )

        except AgentNotFoundError as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            result = WorkItemResult.failure_result(
                work_item=work_item,
                error=e,
                duration_ms=duration_ms,
                lifecycle_state="failed",
                reason="agent_not_found",
            )
            await self._record_execution(work_item, result, started_at)
            await self._record_agent_outcome(work_item.agent_id, False)
            self._record_latency_slo_if_needed(work_item.agent_id, duration_ms)
            raise

        except AgentDisabledError as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            result = WorkItemResult.failure_result(
                work_item=work_item,
                error=e,
                duration_ms=duration_ms,
                lifecycle_state="failed",
                reason="agent_disabled",
            )
            await self._record_execution(work_item, result, started_at)
            await self._record_agent_outcome(work_item.agent_id, False)
            self._record_latency_slo_if_needed(work_item.agent_id, duration_ms)
            raise

        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            result = WorkItemResult.failure_result(
                work_item,
                e,
                duration_ms,
                lifecycle_state="failed",
                reason="pipeline_error",
            )
            await self._record_execution(work_item, result, started_at)
            await self._record_agent_outcome(work_item.agent_id, False)
            self._record_latency_slo_if_needed(work_item.agent_id, duration_ms)
            return result

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
        ai_response.parsed_content = parsed

        should_write, policy_action = self._evaluate_policy(agent, work_item, parsed)
        if not should_write:
            logger.info(
                "Policy prevented writeback",
                agent_id=agent.id,
                document_id=work_item.document_id,
                action=policy_action,
            )
            duration_ms = (time.perf_counter() - start_time) * 1000
            return WorkItemResult.success_result(
                work_item=work_item,
                ai_response=ai_response.to_dict(),
                written=False,
                duration_ms=duration_ms,
                lifecycle_state="write_skipped",
                reason=f"policy_{policy_action.replace(':', '_')}",
            )

        # 5. Write result
        written, write_reason = await self._write_result(agent, work_item, ai_response)

        duration_ms = (time.perf_counter() - start_time) * 1000

        return WorkItemResult.success_result(
            work_item=work_item,
            ai_response=ai_response.to_dict(),
            written=written,
            duration_ms=duration_ms,
            lifecycle_state="written" if written else "write_skipped",
            reason=write_reason,
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
    ) -> tuple[bool, str]:
        """Write the result to MongoDB."""
        if agent.execution.consistency_mode == "shadow":
            get_metrics_collector().record_shadow_write_skip(agent.id)
            logger.info(
                "Shadow mode enabled, skipping writeback",
                agent_id=agent.id,
                document_id=work_item.document_id,
            )
            return False, "shadow_mode"

        if self._result_writer is None:
            if self._mongo_client is None:
                logger.warning(
                    "No MongoDB client, skipping write",
                    agent_id=agent.id,
                )
                return False, "missing_mongo_client"

            self._result_writer = ResultWriter(self._mongo_client)
            await self._result_writer.initialize()

        try:
            return await self._result_writer.write(
                agent=agent,
                document_id=work_item.document_id,
                ai_response=ai_response,
                work_item_id=work_item.id,
                idempotency_key=work_item.idempotency_key,
                source_version=work_item.source_version,
                enforce_strict_version=(
                    agent.execution.consistency_mode == "strict_post_commit"
                ),
                source_document_hash=work_item.source_document_hash,
                enforce_document_hash=(
                    agent.execution.consistency_mode == "strict_post_commit"
                    and agent.execution.require_document_hash_match
                ),
            )
        except Exception as e:
            logger.error(
                "Failed to write result",
                agent_id=agent.id,
                document_id=work_item.document_id,
                error=str(e),
            )
            # Don't fail the whole execution for write errors
            return False, "write_error"

    def _evaluate_policy(
        self,
        agent: AgentConfig,
        work_item: WorkItem,
        parsed: dict[str, Any],
    ) -> tuple[bool, str]:
        """Evaluate policy and return (should_write, action)."""
        policy = agent.policy
        if policy is None:
            return True, "enrich"

        matched = True
        if policy.condition:
            try:
                matched = evaluate_policy_condition(
                    policy.condition,
                    {"document": work_item.document, "result": parsed},
                )
            except PolicyEvaluationError as exc:
                logger.warning(
                    "Policy evaluation failed, using fallback",
                    agent_id=agent.id,
                    condition=policy.condition,
                    error=str(exc),
                )
                matched = False

        action = policy.action if matched else policy.fallback_action
        get_metrics_collector().record_policy_decision(agent.id, action, matched)

        if policy.simulation_mode:
            return False, f"simulation:{action}"

        if action == "skip" or action == "block":
            return False, action

        if action == "tag":
            parsed[policy.tag_field] = policy.tag_value
            return True, action

        return True, action

    async def _get_agent_semaphore(
        self,
        agent_id: str,
        max_concurrency: int | None,
    ) -> asyncio.Semaphore | None:
        """Get/create a semaphore for per-agent concurrency control."""
        if max_concurrency is None:
            return None

        async with _semaphore_lock:
            existing = _agent_semaphores.get(agent_id)
            if existing is None or existing[0] != max_concurrency:
                _agent_semaphores[agent_id] = (max_concurrency, asyncio.Semaphore(max_concurrency))
            return _agent_semaphores[agent_id][1]

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

    async def _record_execution(
        self,
        work_item: WorkItem,
        result: WorkItemResult,
        started_at: datetime,
    ) -> None:
        """Persist execution lifecycle state for auditability."""
        if self._mongo_client is None:
            return

        try:
            collection = self._mongo_client[self._settings.mongodb.database][
                self._settings.mongodb.executions_collection
            ]
            status = "completed" if result.success else "failed"
            if result.success and not result.written:
                status = "skipped"

            await collection.update_one(
                {"_id": work_item.id},
                {"$set": {
                    "agent_id": work_item.agent_id,
                    "document_id": work_item.document_id,
                    "work_item_id": work_item.id,
                    "status": status,
                    "lifecycle_state": result.lifecycle_state,
                    "reason": result.reason,
                    "started_at": started_at,
                    "completed_at": datetime.utcnow(),
                    "duration_ms": result.duration_ms,
                    "attempt": result.attempt,
                    "written": result.written,
                    "error": result.error,
                    "error_type": result.error_type,
                    "ai_response": result.ai_response,
                }},
                upsert=True,
            )
        except Exception as exc:
            logger.warning(
                "Failed to record execution history",
                work_item_id=work_item.id,
                error=str(exc),
            )

    async def _is_agent_quarantined(self, agent_id: str) -> bool:
        """Check and maintain in-process quarantine state for an agent."""
        now = time.monotonic()
        async with _agent_budget_lock:
            until = _agent_quarantine_until.get(agent_id, 0.0)
            if until <= now:
                if agent_id in _agent_quarantine_until:
                    _agent_quarantine_until.pop(agent_id, None)
                    get_metrics_collector().set_agent_quarantine_active(agent_id, False)
                return False
            return True

    async def _record_agent_outcome(self, agent_id: str, success: bool) -> None:
        """Record outcome and enforce per-agent failure budget."""
        if success:
            return

        settings = self._settings.worker
        now = time.monotonic()
        window = settings.agent_error_budget_window_seconds
        threshold = settings.agent_error_budget_max_failures
        quarantine_seconds = settings.agent_quarantine_seconds

        async with _agent_budget_lock:
            failures = _agent_failure_events.setdefault(agent_id, deque())
            failures.append(now)
            cutoff = now - window
            while failures and failures[0] < cutoff:
                failures.popleft()

            if len(failures) < threshold:
                return

            _agent_quarantine_until[agent_id] = now + quarantine_seconds
            failures.clear()
            get_metrics_collector().record_agent_quarantine_event(agent_id)
            get_metrics_collector().set_agent_quarantine_active(agent_id, True)
            logger.warning(
                "Agent entered temporary quarantine",
                agent_id=agent_id,
                quarantine_seconds=quarantine_seconds,
                threshold=threshold,
                window_seconds=window,
            )

    def _record_latency_slo_if_needed(self, agent_id: str, duration_ms: float) -> None:
        """Record latency SLO violations."""
        if duration_ms <= self._settings.worker.latency_slo_ms:
            return
        get_metrics_collector().record_agent_latency_slo_violation(agent_id)
