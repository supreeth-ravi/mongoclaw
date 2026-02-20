"""Agent CRUD endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from mongoclaw.agents.models import AgentConfig, AgentSummary
from mongoclaw.agents.validator import AgentValidator
from mongoclaw.api.dependencies import AgentStoreDep, ApiKeyDep, PaginationDep
from mongoclaw.core.exceptions import AgentNotFoundError, ValidationError

router = APIRouter()


class AgentCreateRequest(BaseModel):
    """Request body for creating an agent."""

    id: str
    name: str
    description: str | None = None
    watch: dict[str, Any]
    ai: dict[str, Any]
    write: dict[str, Any] | None = None
    execution: dict[str, Any] | None = None
    enabled: bool = True
    tags: list[str] = []
    metadata: dict[str, Any] = {}


class AgentUpdateRequest(BaseModel):
    """Request body for updating an agent."""

    name: str | None = None
    description: str | None = None
    watch: dict[str, Any] | None = None
    ai: dict[str, Any] | None = None
    write: dict[str, Any] | None = None
    execution: dict[str, Any] | None = None
    enabled: bool | None = None
    tags: list[str] | None = None
    metadata: dict[str, Any] | None = None


class AgentResponse(BaseModel):
    """Response model for agent operations."""

    success: bool
    agent: AgentConfig | None = None
    message: str | None = None


class AgentListResponse(BaseModel):
    """Response model for agent listing."""

    agents: list[AgentSummary]
    total: int
    skip: int
    limit: int


@router.get("")
async def list_agents(
    store: AgentStoreDep,
    pagination: PaginationDep,
    _api_key: ApiKeyDep,
    enabled_only: bool = False,
    tags: str | None = None,
) -> AgentListResponse:
    """List all agents."""
    tag_list = tags.split(",") if tags else None

    agents = await store.list(
        enabled_only=enabled_only,
        tags=tag_list,
        skip=pagination.skip,
        limit=pagination.limit,
    )

    summaries = [AgentSummary.from_config(a) for a in agents]
    total = await store.count(enabled_only=enabled_only)

    return AgentListResponse(
        agents=summaries,
        total=total,
        skip=pagination.skip,
        limit=pagination.limit,
    )


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_agent(
    request: AgentCreateRequest,
    store: AgentStoreDep,
    _api_key: ApiKeyDep,
) -> AgentResponse:
    """Create a new agent."""
    try:
        # Build full config
        config_data = request.model_dump(exclude_none=True)
        config = AgentConfig.model_validate(config_data)

        # Validate
        validator = AgentValidator()
        validator.validate_or_raise(config)

        # Create
        created = await store.create(config)

        return AgentResponse(success=True, agent=created)

    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get("/{agent_id}")
async def get_agent(
    agent_id: str,
    store: AgentStoreDep,
    _api_key: ApiKeyDep,
) -> AgentConfig:
    """Get an agent by ID."""
    try:
        return await store.get(agent_id)
    except AgentNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Agent '{agent_id}' not found",
        )


@router.put("/{agent_id}")
async def update_agent(
    agent_id: str,
    request: AgentUpdateRequest,
    store: AgentStoreDep,
    _api_key: ApiKeyDep,
) -> AgentResponse:
    """Update an agent."""
    try:
        # Get existing agent
        existing = await store.get(agent_id)

        # Merge updates
        update_data = request.model_dump(exclude_none=True)
        merged_data = existing.model_dump()

        for key, value in update_data.items():
            if isinstance(value, dict) and key in merged_data:
                merged_data[key].update(value)
            else:
                merged_data[key] = value

        # Validate
        config = AgentConfig.model_validate(merged_data)
        validator = AgentValidator()
        validator.validate_or_raise(config)

        # Update
        updated = await store.update(config)

        return AgentResponse(success=True, agent=updated)

    except AgentNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Agent '{agent_id}' not found",
        )
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/{agent_id}")
async def delete_agent(
    agent_id: str,
    store: AgentStoreDep,
    _api_key: ApiKeyDep,
) -> AgentResponse:
    """Delete an agent."""
    deleted = await store.delete(agent_id)

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Agent '{agent_id}' not found",
        )

    return AgentResponse(success=True, message=f"Agent '{agent_id}' deleted")


@router.post("/{agent_id}/enable")
async def enable_agent(
    agent_id: str,
    store: AgentStoreDep,
    _api_key: ApiKeyDep,
) -> AgentResponse:
    """Enable an agent."""
    updated = await store.enable(agent_id)

    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Agent '{agent_id}' not found",
        )

    return AgentResponse(success=True, message=f"Agent '{agent_id}' enabled")


@router.post("/{agent_id}/disable")
async def disable_agent(
    agent_id: str,
    store: AgentStoreDep,
    _api_key: ApiKeyDep,
) -> AgentResponse:
    """Disable an agent."""
    updated = await store.disable(agent_id)

    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Agent '{agent_id}' not found",
        )

    return AgentResponse(success=True, message=f"Agent '{agent_id}' disabled")


@router.post("/{agent_id}/validate")
async def validate_agent(
    agent_id: str,
    store: AgentStoreDep,
    _api_key: ApiKeyDep,
) -> dict[str, Any]:
    """Validate an agent configuration."""
    try:
        agent = await store.get(agent_id)

        validator = AgentValidator()
        errors = validator.validate(agent)

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "agent_id": agent_id,
        }

    except AgentNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Agent '{agent_id}' not found",
        )
