"""Cost and token tracking for AI usage."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from mongoclaw.core.exceptions import CostLimitExceededError
from mongoclaw.core.types import AIResponse
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)


class CostTracker:
    """
    Tracks AI usage costs and tokens.

    Features:
    - Per-agent tracking
    - Time-windowed limits
    - Cost alerts
    """

    def __init__(
        self,
        global_cost_limit: float | None = None,
        global_token_limit: int | None = None,
        window_hours: int = 24,
    ) -> None:
        self._global_cost_limit = global_cost_limit
        self._global_token_limit = global_token_limit
        self._window = timedelta(hours=window_hours)

        # Global tracking
        self._total_cost = 0.0
        self._total_tokens = 0
        self._request_count = 0
        self._window_start = datetime.utcnow()

        # Per-agent tracking
        self._agent_costs: dict[str, float] = {}
        self._agent_tokens: dict[str, int] = {}
        self._agent_requests: dict[str, int] = {}

        # Per-model tracking
        self._model_costs: dict[str, float] = {}
        self._model_tokens: dict[str, int] = {}

        # Agent limits
        self._agent_cost_limits: dict[str, float] = {}
        self._agent_token_limits: dict[str, int] = {}

    def track(
        self,
        response: AIResponse,
        agent_id: str | None = None,
    ) -> None:
        """
        Track usage from an AI response.

        Args:
            response: The AI response with usage data.
            agent_id: Optional agent identifier.
        """
        self._maybe_reset_window()

        # Update global totals
        self._total_cost += response.cost_usd
        self._total_tokens += response.total_tokens
        self._request_count += 1

        # Update model tracking
        model = response.model
        self._model_costs[model] = self._model_costs.get(model, 0.0) + response.cost_usd
        self._model_tokens[model] = self._model_tokens.get(model, 0) + response.total_tokens

        # Update agent tracking
        if agent_id:
            self._agent_costs[agent_id] = (
                self._agent_costs.get(agent_id, 0.0) + response.cost_usd
            )
            self._agent_tokens[agent_id] = (
                self._agent_tokens.get(agent_id, 0) + response.total_tokens
            )
            self._agent_requests[agent_id] = (
                self._agent_requests.get(agent_id, 0) + 1
            )

        logger.debug(
            "Tracked AI usage",
            agent_id=agent_id,
            cost=response.cost_usd,
            tokens=response.total_tokens,
            model=model,
        )

    def check_limits(self, agent_id: str | None = None) -> None:
        """
        Check if any limits are exceeded.

        Args:
            agent_id: Optional agent to check.

        Raises:
            CostLimitExceededError: If limits exceeded.
        """
        self._maybe_reset_window()

        # Check global cost limit
        if self._global_cost_limit:
            if self._total_cost >= self._global_cost_limit:
                raise CostLimitExceededError(
                    limit_type="global_cost",
                    current_value=self._total_cost,
                    limit_value=self._global_cost_limit,
                )

        # Check global token limit
        if self._global_token_limit:
            if self._total_tokens >= self._global_token_limit:
                raise CostLimitExceededError(
                    limit_type="global_tokens",
                    current_value=float(self._total_tokens),
                    limit_value=float(self._global_token_limit),
                )

        # Check agent-specific limits
        if agent_id:
            if agent_id in self._agent_cost_limits:
                limit = self._agent_cost_limits[agent_id]
                current = self._agent_costs.get(agent_id, 0.0)
                if current >= limit:
                    raise CostLimitExceededError(
                        limit_type="agent_cost",
                        current_value=current,
                        limit_value=limit,
                        agent_id=agent_id,
                    )

            if agent_id in self._agent_token_limits:
                limit = self._agent_token_limits[agent_id]
                current = self._agent_tokens.get(agent_id, 0)
                if current >= limit:
                    raise CostLimitExceededError(
                        limit_type="agent_tokens",
                        current_value=float(current),
                        limit_value=float(limit),
                        agent_id=agent_id,
                    )

    def set_agent_limit(
        self,
        agent_id: str,
        cost_limit: float | None = None,
        token_limit: int | None = None,
    ) -> None:
        """
        Set limits for a specific agent.

        Args:
            agent_id: The agent identifier.
            cost_limit: Cost limit in USD.
            token_limit: Token limit.
        """
        if cost_limit is not None:
            self._agent_cost_limits[agent_id] = cost_limit
        if token_limit is not None:
            self._agent_token_limits[agent_id] = token_limit

    def _maybe_reset_window(self) -> None:
        """Reset tracking if window has expired."""
        now = datetime.utcnow()
        if now - self._window_start > self._window:
            logger.info(
                "Resetting cost tracker window",
                previous_cost=self._total_cost,
                previous_tokens=self._total_tokens,
            )
            self._reset()

    def _reset(self) -> None:
        """Reset all tracking."""
        self._total_cost = 0.0
        self._total_tokens = 0
        self._request_count = 0
        self._window_start = datetime.utcnow()

        self._agent_costs.clear()
        self._agent_tokens.clear()
        self._agent_requests.clear()
        self._model_costs.clear()
        self._model_tokens.clear()

    def get_stats(self) -> dict[str, Any]:
        """Get overall statistics."""
        self._maybe_reset_window()

        return {
            "window_start": self._window_start.isoformat(),
            "window_hours": self._window.total_seconds() / 3600,
            "total_cost_usd": round(self._total_cost, 6),
            "total_tokens": self._total_tokens,
            "request_count": self._request_count,
            "global_cost_limit": self._global_cost_limit,
            "global_token_limit": self._global_token_limit,
            "cost_utilization": (
                self._total_cost / self._global_cost_limit
                if self._global_cost_limit
                else None
            ),
        }

    def get_agent_stats(self, agent_id: str) -> dict[str, Any]:
        """Get statistics for a specific agent."""
        return {
            "agent_id": agent_id,
            "cost_usd": round(self._agent_costs.get(agent_id, 0.0), 6),
            "tokens": self._agent_tokens.get(agent_id, 0),
            "requests": self._agent_requests.get(agent_id, 0),
            "cost_limit": self._agent_cost_limits.get(agent_id),
            "token_limit": self._agent_token_limits.get(agent_id),
        }

    def get_model_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics per model."""
        return {
            model: {
                "cost_usd": round(self._model_costs.get(model, 0.0), 6),
                "tokens": self._model_tokens.get(model, 0),
            }
            for model in set(self._model_costs.keys()) | set(self._model_tokens.keys())
        }

    def get_top_agents(self, n: int = 10, by: str = "cost") -> list[dict[str, Any]]:
        """Get top agents by cost or tokens."""
        if by == "cost":
            sorted_agents = sorted(
                self._agent_costs.items(),
                key=lambda x: x[1],
                reverse=True,
            )[:n]
        else:
            sorted_agents = sorted(
                self._agent_tokens.items(),
                key=lambda x: x[1],
                reverse=True,
            )[:n]

        return [
            {
                "agent_id": agent_id,
                "cost_usd": round(self._agent_costs.get(agent_id, 0.0), 6),
                "tokens": self._agent_tokens.get(agent_id, 0),
                "requests": self._agent_requests.get(agent_id, 0),
            }
            for agent_id, _ in sorted_agents
        ]
