"""Resume token persistence for change stream crash recovery."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import IndexModel, ASCENDING

from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)


class ResumeTokenStore:
    """
    Persists MongoDB change stream resume tokens for crash recovery.

    Tokens are stored per namespace (database.collection) and can be
    used to resume watching from the last processed event.
    """

    def __init__(
        self,
        client: AsyncIOMotorClient[dict[str, Any]],
        database: str = "mongoclaw",
        collection: str = "resume_tokens",
    ) -> None:
        self._client = client
        self._database_name = database
        self._collection_name = collection
        self._collection: AsyncIOMotorCollection[dict[str, Any]] | None = None

    @property
    def collection(self) -> AsyncIOMotorCollection[dict[str, Any]]:
        """Get collection instance."""
        if self._collection is None:
            self._collection = self._client[self._database_name][self._collection_name]
        return self._collection

    async def initialize(self) -> None:
        """Initialize indexes for the collection."""
        indexes = [
            IndexModel(
                [("database", ASCENDING), ("collection", ASCENDING)],
                unique=True,
            ),
            IndexModel([("updated_at", ASCENDING)]),
        ]
        await self.collection.create_indexes(indexes)
        logger.debug("Resume token store initialized")

    async def save(
        self,
        database: str,
        collection: str,
        token: dict[str, Any],
    ) -> None:
        """
        Save a resume token for a namespace.

        Args:
            database: The database name.
            collection: The collection name.
            token: The resume token to save.
        """
        await self.collection.update_one(
            {
                "database": database,
                "collection": collection,
            },
            {
                "$set": {
                    "token": token,
                    "updated_at": datetime.utcnow(),
                },
                "$setOnInsert": {
                    "database": database,
                    "collection": collection,
                    "created_at": datetime.utcnow(),
                },
            },
            upsert=True,
        )

    async def get(
        self,
        database: str,
        collection: str,
    ) -> dict[str, Any] | None:
        """
        Get the resume token for a namespace.

        Args:
            database: The database name.
            collection: The collection name.

        Returns:
            The resume token or None if not found.
        """
        doc = await self.collection.find_one(
            {
                "database": database,
                "collection": collection,
            }
        )

        if doc:
            return doc.get("token")
        return None

    async def delete(self, database: str, collection: str) -> bool:
        """
        Delete the resume token for a namespace.

        Args:
            database: The database name.
            collection: The collection name.

        Returns:
            True if deleted, False if not found.
        """
        result = await self.collection.delete_one(
            {
                "database": database,
                "collection": collection,
            }
        )
        return result.deleted_count > 0

    async def delete_all(self) -> int:
        """
        Delete all resume tokens.

        Returns:
            Number of tokens deleted.
        """
        result = await self.collection.delete_many({})
        return result.deleted_count

    async def list_all(self) -> list[dict[str, Any]]:
        """
        List all stored resume tokens.

        Returns:
            List of token documents with metadata.
        """
        cursor = self.collection.find({})
        tokens = []

        async for doc in cursor:
            tokens.append({
                "database": doc["database"],
                "collection": doc["collection"],
                "has_token": doc.get("token") is not None,
                "updated_at": doc.get("updated_at"),
            })

        return tokens

    async def get_age_seconds(self, database: str, collection: str) -> float | None:
        """
        Get the age of a resume token in seconds.

        Args:
            database: The database name.
            collection: The collection name.

        Returns:
            Age in seconds or None if not found.
        """
        doc = await self.collection.find_one(
            {
                "database": database,
                "collection": collection,
            },
            projection={"updated_at": 1},
        )

        if doc and doc.get("updated_at"):
            age = datetime.utcnow() - doc["updated_at"]
            return age.total_seconds()

        return None
