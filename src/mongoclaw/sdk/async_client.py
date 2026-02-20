"""Async Python SDK client for MongoClaw."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import httpx
from pydantic import BaseModel


class AgentSummary(BaseModel):
    """Summary of an agent."""

    id: str
    name: str
    enabled: bool
    database: str
    collection: str
    model: str


class AgentDetails(BaseModel):
    """Full agent details."""

    id: str
    name: str
    description: str | None = None
    enabled: bool
    watch: dict[str, Any]
    ai: dict[str, Any]
    write: dict[str, Any]
    execution: dict[str, Any]
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ExecutionRecord(BaseModel):
    """Record of an agent execution."""

    id: str
    agent_id: str
    document_id: str
    status: str
    started_at: datetime
    completed_at: datetime | None = None
    duration_ms: float | None = None
    tokens_used: int | None = None
    cost_usd: float | None = None
    error: str | None = None


class HealthStatus(BaseModel):
    """Health check status."""

    status: str
    version: str | None = None
    environment: str | None = None
    components: dict[str, dict[str, Any]] | None = None


class AsyncMongoClawClient:
    """Async client for interacting with MongoClaw API."""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        api_key: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        """Initialize the async client.

        Args:
            base_url: Base URL of the MongoClaw API.
            api_key: Optional API key for authentication.
            timeout: Request timeout in seconds.
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "AsyncMongoClawClient":
        """Enter async context."""
        await self._ensure_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context."""
        await self.close()

    async def _ensure_client(self) -> httpx.AsyncClient:
        """Ensure HTTP client is created."""
        if self._client is None:
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"

            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=headers,
                timeout=httpx.Timeout(self.timeout),
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs,
    ) -> httpx.Response:
        """Make an HTTP request."""
        client = await self._ensure_client()
        response = await client.request(method, path, **kwargs)
        response.raise_for_status()
        return response

    # Health endpoints

    async def health(self) -> HealthStatus:
        """Check basic health status."""
        response = await self._request("GET", "/health")
        return HealthStatus(**response.json())

    async def health_detailed(self) -> HealthStatus:
        """Get detailed health status with component checks."""
        response = await self._request("GET", "/health/detailed")
        return HealthStatus(**response.json())

    async def is_healthy(self) -> bool:
        """Check if the service is healthy."""
        try:
            status = await self.health()
            return status.status == "healthy"
        except Exception:
            return False

    # Agent endpoints

    async def list_agents(
        self,
        enabled_only: bool = False,
        skip: int = 0,
        limit: int = 100,
    ) -> list[AgentSummary]:
        """List all agents.

        Args:
            enabled_only: Only return enabled agents.
            skip: Number of agents to skip.
            limit: Maximum number of agents to return.

        Returns:
            List of agent summaries.
        """
        params = {
            "enabled_only": enabled_only,
            "skip": skip,
            "limit": limit,
        }
        response = await self._request("GET", "/api/v1/agents", params=params)
        data = response.json()
        return [AgentSummary(**agent) for agent in data.get("agents", [])]

    async def get_agent(self, agent_id: str) -> AgentDetails:
        """Get an agent by ID.

        Args:
            agent_id: The agent ID.

        Returns:
            Full agent details.
        """
        response = await self._request("GET", f"/api/v1/agents/{agent_id}")
        return AgentDetails(**response.json())

    async def create_agent(self, config: dict[str, Any]) -> AgentDetails:
        """Create a new agent.

        Args:
            config: Agent configuration dictionary.

        Returns:
            Created agent details.
        """
        response = await self._request("POST", "/api/v1/agents", json=config)
        return AgentDetails(**response.json())

    async def update_agent(
        self,
        agent_id: str,
        config: dict[str, Any],
    ) -> AgentDetails:
        """Update an agent.

        Args:
            agent_id: The agent ID.
            config: Updated configuration.

        Returns:
            Updated agent details.
        """
        response = await self._request(
            "PUT",
            f"/api/v1/agents/{agent_id}",
            json=config,
        )
        return AgentDetails(**response.json())

    async def delete_agent(self, agent_id: str) -> bool:
        """Delete an agent.

        Args:
            agent_id: The agent ID.

        Returns:
            True if deleted successfully.
        """
        response = await self._request("DELETE", f"/api/v1/agents/{agent_id}")
        return response.status_code == 204

    async def enable_agent(self, agent_id: str) -> AgentDetails:
        """Enable an agent.

        Args:
            agent_id: The agent ID.

        Returns:
            Updated agent details.
        """
        response = await self._request("POST", f"/api/v1/agents/{agent_id}/enable")
        return AgentDetails(**response.json())

    async def disable_agent(self, agent_id: str) -> AgentDetails:
        """Disable an agent.

        Args:
            agent_id: The agent ID.

        Returns:
            Updated agent details.
        """
        response = await self._request("POST", f"/api/v1/agents/{agent_id}/disable")
        return AgentDetails(**response.json())

    async def validate_agent(self, config: dict[str, Any]) -> dict[str, Any]:
        """Validate an agent configuration.

        Args:
            config: Agent configuration to validate.

        Returns:
            Validation result with any errors.
        """
        response = await self._request("POST", "/api/v1/agents/validate", json=config)
        return response.json()

    # Execution endpoints

    async def list_executions(
        self,
        agent_id: str | None = None,
        status: str | None = None,
        skip: int = 0,
        limit: int = 100,
    ) -> list[ExecutionRecord]:
        """List execution history.

        Args:
            agent_id: Filter by agent ID.
            status: Filter by status.
            skip: Number of records to skip.
            limit: Maximum number of records to return.

        Returns:
            List of execution records.
        """
        params = {"skip": skip, "limit": limit}
        if agent_id:
            params["agent_id"] = agent_id
        if status:
            params["status"] = status

        response = await self._request("GET", "/api/v1/executions", params=params)
        data = response.json()
        return [ExecutionRecord(**record) for record in data.get("executions", [])]

    async def get_execution(self, execution_id: str) -> ExecutionRecord:
        """Get an execution by ID.

        Args:
            execution_id: The execution ID.

        Returns:
            Execution record.
        """
        response = await self._request("GET", f"/api/v1/executions/{execution_id}")
        return ExecutionRecord(**response.json())

    async def retry_execution(self, execution_id: str) -> ExecutionRecord:
        """Retry a failed execution.

        Args:
            execution_id: The execution ID to retry.

        Returns:
            New execution record.
        """
        response = await self._request(
            "POST",
            f"/api/v1/executions/{execution_id}/retry",
        )
        return ExecutionRecord(**response.json())

    # Metrics endpoints

    async def get_metrics(self) -> dict[str, Any]:
        """Get current metrics."""
        response = await self._request("GET", "/metrics")
        return {"raw": response.text}

    async def get_agent_stats(self, agent_id: str) -> dict[str, Any]:
        """Get statistics for a specific agent.

        Args:
            agent_id: The agent ID.

        Returns:
            Agent statistics.
        """
        response = await self._request("GET", f"/api/v1/agents/{agent_id}/stats")
        return response.json()

    # Webhook endpoints

    async def trigger_agent(
        self,
        agent_id: str,
        document: dict[str, Any],
    ) -> ExecutionRecord:
        """Manually trigger an agent for a document.

        Args:
            agent_id: The agent ID.
            document: The document to process.

        Returns:
            Execution record.
        """
        response = await self._request(
            "POST",
            f"/api/v1/webhooks/trigger/{agent_id}",
            json={"document": document},
        )
        return ExecutionRecord(**response.json())

    # Utility methods

    async def wait_for_execution(
        self,
        execution_id: str,
        timeout: float = 60.0,
        poll_interval: float = 1.0,
    ) -> ExecutionRecord:
        """Wait for an execution to complete.

        Args:
            execution_id: The execution ID.
            timeout: Maximum time to wait in seconds.
            poll_interval: Time between polls in seconds.

        Returns:
            Final execution record.

        Raises:
            TimeoutError: If execution doesn't complete in time.
        """
        start = asyncio.get_event_loop().time()

        while True:
            execution = await self.get_execution(execution_id)
            if execution.status in ("completed", "failed"):
                return execution

            elapsed = asyncio.get_event_loop().time() - start
            if elapsed >= timeout:
                raise TimeoutError(
                    f"Execution {execution_id} did not complete within {timeout}s"
                )

            await asyncio.sleep(poll_interval)
