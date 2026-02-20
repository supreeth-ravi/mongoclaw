"""Pytest configuration and fixtures."""

from __future__ import annotations

import asyncio
import os
from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio


# Set test environment
os.environ.setdefault("MONGOCLAW_ENVIRONMENT", "test")
os.environ.setdefault("MONGOCLAW_MONGODB_URI", "mongodb://localhost:27017/mongoclaw_test")
os.environ.setdefault("MONGOCLAW_REDIS_URL", "redis://localhost:6379/1")


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def sample_agent_config() -> dict:
    """Sample agent configuration for testing."""
    return {
        "id": "test_agent",
        "name": "Test Agent",
        "description": "Agent for testing",
        "watch": {
            "database": "test_db",
            "collection": "test_collection",
            "operations": ["insert"],
        },
        "ai": {
            "model": "gpt-4o-mini",
            "prompt": "Test prompt: {{ document.title }}",
            "temperature": 0.5,
            "max_tokens": 100,
        },
        "write": {
            "strategy": "merge",
        },
        "execution": {
            "max_retries": 3,
            "retry_delay_seconds": 1.0,
            "timeout_seconds": 30.0,
        },
        "enabled": True,
    }


@pytest.fixture
def sample_document() -> dict:
    """Sample document for testing."""
    return {
        "_id": "doc_123",
        "title": "Test Document",
        "content": "This is test content for AI processing.",
        "status": "pending",
    }


@pytest.fixture
def sample_change_event(sample_document: dict) -> dict:
    """Sample MongoDB change event for testing."""
    return {
        "operationType": "insert",
        "fullDocument": sample_document,
        "ns": {"db": "test_db", "coll": "test_collection"},
        "documentKey": {"_id": sample_document["_id"]},
    }


@pytest_asyncio.fixture
async def mongo_client() -> AsyncGenerator:
    """Create a MongoDB client for testing."""
    from motor.motor_asyncio import AsyncIOMotorClient
    from mongoclaw.core.config import get_settings

    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongodb.uri.get_secret_value())

    yield client

    client.close()


@pytest_asyncio.fixture
async def redis_client() -> AsyncGenerator:
    """Create a Redis client for testing."""
    import redis.asyncio as redis
    from mongoclaw.core.config import get_settings

    settings = get_settings()
    client = redis.from_url(settings.redis.url.get_secret_value())

    yield client

    await client.close()


@pytest_asyncio.fixture
async def agent_store(mongo_client) -> AsyncGenerator:
    """Create an agent store for testing."""
    from mongoclaw.agents.store import AgentStore
    from mongoclaw.core.config import get_settings

    settings = get_settings()
    store = AgentStore(
        client=mongo_client,
        database=settings.mongodb.database + "_test",
        collection="agents_test",
    )
    await store.initialize()

    yield store

    # Cleanup
    await mongo_client[settings.mongodb.database + "_test"]["agents_test"].drop()
