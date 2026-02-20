"""FastAPI dependency injection providers."""

from __future__ import annotations

from typing import Annotated, Any, AsyncIterator

from fastapi import Depends, Header, HTTPException, Request, status
from motor.motor_asyncio import AsyncIOMotorClient

from mongoclaw.agents.store import AgentStore
from mongoclaw.core.config import Settings, get_settings
from mongoclaw.core.exceptions import AuthenticationError
from mongoclaw.security.auth import verify_api_key


async def get_settings_dep() -> Settings:
    """Dependency for settings."""
    return get_settings()


SettingsDep = Annotated[Settings, Depends(get_settings_dep)]


async def get_mongo_client(request: Request) -> AsyncIOMotorClient[dict[str, Any]]:
    """Get MongoDB client from app state."""
    if not hasattr(request.app.state, "mongo_client"):
        settings = get_settings()
        request.app.state.mongo_client = AsyncIOMotorClient(
            settings.mongodb.uri.get_secret_value()
        )
    return request.app.state.mongo_client


MongoClientDep = Annotated[AsyncIOMotorClient[dict[str, Any]], Depends(get_mongo_client)]


async def get_agent_store(
    client: MongoClientDep,
    settings: SettingsDep,
) -> AgentStore:
    """Get agent store instance."""
    store = AgentStore(
        client=client,
        database=settings.mongodb.database,
        collection=settings.mongodb.agents_collection,
    )
    return store


AgentStoreDep = Annotated[AgentStore, Depends(get_agent_store)]


async def verify_api_key_header(
    x_api_key: str | None = Header(None, alias="X-API-Key"),
) -> str:
    """
    Dependency for API key verification.

    Returns the API key if valid.
    """
    try:
        await verify_api_key(x_api_key)
        return x_api_key or ""
    except AuthenticationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "ApiKey"},
        )


ApiKeyDep = Annotated[str, Depends(verify_api_key_header)]


async def get_optional_api_key(
    x_api_key: str | None = Header(None, alias="X-API-Key"),
) -> str | None:
    """
    Dependency for optional API key.

    Returns the key without validation for public endpoints.
    """
    return x_api_key


OptionalApiKeyDep = Annotated[str | None, Depends(get_optional_api_key)]


class Pagination:
    """Pagination parameters."""

    def __init__(
        self,
        skip: int = 0,
        limit: int = 100,
    ) -> None:
        self.skip = max(0, skip)
        self.limit = min(max(1, limit), 1000)


async def get_pagination(
    skip: int = 0,
    limit: int = 100,
) -> Pagination:
    """Get pagination parameters."""
    return Pagination(skip=skip, limit=limit)


PaginationDep = Annotated[Pagination, Depends(get_pagination)]


class CurrentUser:
    """Current authenticated user context."""

    def __init__(
        self,
        user_id: str | None = None,
        api_key: str | None = None,
        roles: list[str] | None = None,
    ) -> None:
        self.user_id = user_id or "anonymous"
        self.api_key = api_key
        self.roles = roles or []

    @property
    def is_authenticated(self) -> bool:
        return self.user_id != "anonymous"


async def get_current_user(
    api_key: ApiKeyDep,
) -> CurrentUser:
    """Get current user from API key."""
    # In a real implementation, this would look up user info
    return CurrentUser(
        user_id="api_user",
        api_key=api_key,
        roles=["developer"],
    )


CurrentUserDep = Annotated[CurrentUser, Depends(get_current_user)]
