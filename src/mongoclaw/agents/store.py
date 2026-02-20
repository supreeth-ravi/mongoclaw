"""MongoDB store for agent configurations."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import IndexModel, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError

from mongoclaw.agents.models import AgentConfig, AgentSummary
from mongoclaw.core.exceptions import AgentNotFoundError, ValidationError


class AgentStore:
    """MongoDB store for agent configurations with CRUD operations."""

    def __init__(
        self,
        client: AsyncIOMotorClient[dict[str, Any]],
        database: str = "mongoclaw",
        collection: str = "agents",
    ) -> None:
        self._client = client
        self._database_name = database
        self._collection_name = collection
        self._db: AsyncIOMotorDatabase[dict[str, Any]] | None = None
        self._collection: AsyncIOMotorCollection[dict[str, Any]] | None = None

    @property
    def db(self) -> AsyncIOMotorDatabase[dict[str, Any]]:
        """Get database instance."""
        if self._db is None:
            self._db = self._client[self._database_name]
        return self._db

    @property
    def collection(self) -> AsyncIOMotorCollection[dict[str, Any]]:
        """Get collection instance."""
        if self._collection is None:
            self._collection = self.db[self._collection_name]
        return self._collection

    async def initialize(self) -> None:
        """Initialize indexes and collection setup."""
        indexes = [
            IndexModel([("enabled", ASCENDING)]),
            IndexModel([("watch.database", ASCENDING), ("watch.collection", ASCENDING)]),
            IndexModel([("tags", ASCENDING)]),
            IndexModel([("created_at", DESCENDING)]),
            IndexModel([("updated_at", DESCENDING)]),
        ]
        await self.collection.create_indexes(indexes)

    async def create(self, config: AgentConfig) -> AgentConfig:
        """
        Create a new agent configuration.

        Args:
            config: The agent configuration to create.

        Returns:
            The created configuration with timestamps.

        Raises:
            ValidationError: If agent with same ID already exists.
        """
        config.created_at = datetime.utcnow()
        config.updated_at = config.created_at
        config.version = 1

        doc = config.to_mongo_doc()

        try:
            await self.collection.insert_one(doc)
        except DuplicateKeyError:
            raise ValidationError(
                f"Agent with ID '{config.id}' already exists",
                details={"agent_id": config.id},
            )

        return config

    async def get(self, agent_id: str) -> AgentConfig:
        """
        Get an agent configuration by ID.

        Args:
            agent_id: The agent ID.

        Returns:
            The agent configuration.

        Raises:
            AgentNotFoundError: If agent is not found.
        """
        doc = await self.collection.find_one({"_id": agent_id})
        if doc is None:
            raise AgentNotFoundError(agent_id)
        return AgentConfig.from_mongo_doc(doc)

    async def get_optional(self, agent_id: str) -> AgentConfig | None:
        """
        Get an agent configuration by ID, returning None if not found.

        Args:
            agent_id: The agent ID.

        Returns:
            The agent configuration or None.
        """
        doc = await self.collection.find_one({"_id": agent_id})
        if doc is None:
            return None
        return AgentConfig.from_mongo_doc(doc)

    async def update(self, config: AgentConfig) -> AgentConfig:
        """
        Update an existing agent configuration.

        Args:
            config: The updated configuration.

        Returns:
            The updated configuration.

        Raises:
            AgentNotFoundError: If agent is not found.
        """
        config.updated_at = datetime.utcnow()
        config.version += 1

        doc = config.to_mongo_doc()
        doc_id = doc.pop("_id")

        result = await self.collection.update_one(
            {"_id": doc_id},
            {"$set": doc},
        )

        if result.matched_count == 0:
            raise AgentNotFoundError(config.id)

        return config

    async def delete(self, agent_id: str) -> bool:
        """
        Delete an agent configuration.

        Args:
            agent_id: The agent ID.

        Returns:
            True if deleted, False if not found.
        """
        result = await self.collection.delete_one({"_id": agent_id})
        return result.deleted_count > 0

    async def list(
        self,
        enabled_only: bool = False,
        tags: list[str] | None = None,
        database: str | None = None,
        collection: str | None = None,
        skip: int = 0,
        limit: int = 100,
    ) -> list[AgentConfig]:
        """
        List agent configurations with optional filters.

        Args:
            enabled_only: Only return enabled agents.
            tags: Filter by tags (any match).
            database: Filter by watch database.
            collection: Filter by watch collection.
            skip: Number of results to skip.
            limit: Maximum results to return.

        Returns:
            List of matching agent configurations.
        """
        query: dict[str, Any] = {}

        if enabled_only:
            query["enabled"] = True

        if tags:
            query["tags"] = {"$in": tags}

        if database:
            query["watch.database"] = database

        if collection:
            query["watch.collection"] = collection

        cursor = self.collection.find(query).skip(skip).limit(limit)
        cursor = cursor.sort("created_at", DESCENDING)

        agents = []
        async for doc in cursor:
            agents.append(AgentConfig.from_mongo_doc(doc))

        return agents

    async def list_summaries(
        self,
        enabled_only: bool = False,
        skip: int = 0,
        limit: int = 100,
    ) -> list[AgentSummary]:
        """
        List agent summaries (lightweight view).

        Args:
            enabled_only: Only return enabled agents.
            skip: Number of results to skip.
            limit: Maximum results to return.

        Returns:
            List of agent summaries.
        """
        agents = await self.list(enabled_only=enabled_only, skip=skip, limit=limit)
        return [AgentSummary.from_config(a) for a in agents]

    async def count(self, enabled_only: bool = False) -> int:
        """
        Count agent configurations.

        Args:
            enabled_only: Only count enabled agents.

        Returns:
            Number of matching agents.
        """
        query: dict[str, Any] = {}
        if enabled_only:
            query["enabled"] = True
        return await self.collection.count_documents(query)

    async def get_by_watch_target(
        self,
        database: str,
        collection: str,
        enabled_only: bool = True,
    ) -> list[AgentConfig]:
        """
        Get all agents watching a specific database and collection.

        Args:
            database: The database name.
            collection: The collection name.
            enabled_only: Only return enabled agents.

        Returns:
            List of matching agent configurations.
        """
        query: dict[str, Any] = {
            "watch.database": database,
            "watch.collection": collection,
        }
        if enabled_only:
            query["enabled"] = True

        cursor = self.collection.find(query)

        agents = []
        async for doc in cursor:
            agents.append(AgentConfig.from_mongo_doc(doc))

        return agents

    async def get_all_watch_targets(
        self,
        enabled_only: bool = True,
    ) -> list[tuple[str, str]]:
        """
        Get all unique (database, collection) pairs being watched.

        Args:
            enabled_only: Only consider enabled agents.

        Returns:
            List of (database, collection) tuples.
        """
        match_stage: dict[str, Any] = {}
        if enabled_only:
            match_stage["enabled"] = True

        pipeline: list[dict[str, Any]] = []
        if match_stage:
            pipeline.append({"$match": match_stage})

        pipeline.extend([
            {
                "$group": {
                    "_id": {
                        "database": "$watch.database",
                        "collection": "$watch.collection",
                    }
                }
            },
            {"$sort": {"_id.database": 1, "_id.collection": 1}},
        ])

        cursor = self.collection.aggregate(pipeline)

        targets = []
        async for doc in cursor:
            targets.append((doc["_id"]["database"], doc["_id"]["collection"]))

        return targets

    async def enable(self, agent_id: str) -> bool:
        """
        Enable an agent.

        Args:
            agent_id: The agent ID.

        Returns:
            True if updated, False if not found.
        """
        result = await self.collection.update_one(
            {"_id": agent_id},
            {
                "$set": {
                    "enabled": True,
                    "updated_at": datetime.utcnow(),
                },
                "$inc": {"version": 1},
            },
        )
        return result.matched_count > 0

    async def disable(self, agent_id: str) -> bool:
        """
        Disable an agent.

        Args:
            agent_id: The agent ID.

        Returns:
            True if updated, False if not found.
        """
        result = await self.collection.update_one(
            {"_id": agent_id},
            {
                "$set": {
                    "enabled": False,
                    "updated_at": datetime.utcnow(),
                },
                "$inc": {"version": 1},
            },
        )
        return result.matched_count > 0

    async def get_versions(self, agent_id: str, limit: int = 10) -> list[dict[str, Any]]:
        """
        Get version history for an agent (requires versioning collection).

        This is a placeholder for future version history tracking.
        """
        # TODO: Implement version history tracking
        return []
