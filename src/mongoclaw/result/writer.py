"""Idempotent result writer for MongoDB."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import IndexModel, ASCENDING
from pymongo.errors import DuplicateKeyError

from mongoclaw.agents.models import AgentConfig, WriteConfig
from mongoclaw.core.exceptions import IdempotencyError, WriteConflictError
from mongoclaw.core.types import AIResponse, WriteStrategy
from mongoclaw.observability.logging import get_logger
from mongoclaw.observability.metrics import get_metrics_collector
from mongoclaw.result.strategies import WriteStrategyHandler

logger = get_logger(__name__)


class ResultWriter:
    """
    Writes AI results back to MongoDB with idempotency.

    Features:
    - Multiple write strategies (merge, replace, append, nested)
    - Idempotency key tracking
    - Atomic updates with optimistic locking
    - Execution metadata tracking
    """

    def __init__(
        self,
        client: AsyncIOMotorClient[dict[str, Any]],
        idempotency_database: str = "mongoclaw",
        idempotency_collection: str = "idempotency_keys",
        idempotency_ttl_seconds: int = 86400,  # 24 hours
    ) -> None:
        self._client = client
        self._idempotency_db = idempotency_database
        self._idempotency_coll = idempotency_collection
        self._idempotency_ttl = idempotency_ttl_seconds

        self._strategy_handler = WriteStrategyHandler()
        self._idempotency_collection: AsyncIOMotorCollection[dict[str, Any]] | None = None

    @property
    def idempotency_collection(self) -> AsyncIOMotorCollection[dict[str, Any]]:
        """Get the idempotency collection."""
        if self._idempotency_collection is None:
            self._idempotency_collection = self._client[self._idempotency_db][
                self._idempotency_coll
            ]
        return self._idempotency_collection

    async def initialize(self) -> None:
        """Initialize indexes."""
        indexes = [
            IndexModel([("key", ASCENDING)], unique=True),
            IndexModel(
                [("created_at", ASCENDING)],
                expireAfterSeconds=self._idempotency_ttl,
            ),
        ]
        await self.idempotency_collection.create_indexes(indexes)
        logger.debug("Result writer initialized")

    async def write(
        self,
        agent: AgentConfig,
        document_id: str,
        ai_response: AIResponse,
        work_item_id: str,
        idempotency_key: str | None = None,
        source_version: int | None = None,
        enforce_strict_version: bool = False,
        source_document_hash: str | None = None,
        enforce_document_hash: bool = False,
    ) -> tuple[bool, str]:
        """
        Write AI result to MongoDB.

        Args:
            agent: The agent configuration.
            document_id: The document _id to update.
            ai_response: The AI response with parsed content.
            work_item_id: The work item ID for tracking.
            idempotency_key: Optional idempotency key.
            source_version: Dispatch-time `_mongoclaw_version` value, if known.
            enforce_strict_version: Whether to enforce optimistic version checks.

        Returns:
            Tuple of (was_written, reason_code).

        Raises:
            IdempotencyError: If idempotency check fails.
            WriteConflictError: If document was modified.
        """
        write_config = agent.write

        # Check idempotency
        if idempotency_key:
            if await self._check_idempotency(idempotency_key):
                logger.debug(
                    "Skipping duplicate write",
                    agent_id=agent.id,
                    document_id=document_id,
                    idempotency_key=idempotency_key,
                )
                return False, "idempotency_duplicate"

        # Get target database and collection
        target_db = write_config.target_database or agent.watch.database
        target_coll = write_config.target_collection or agent.watch.collection
        collection = self._client[target_db][target_coll]

        # Build update document
        parsed_content = ai_response.parsed_content or {"content": ai_response.content}
        update = self._build_update(
            write_config=write_config,
            parsed_content=parsed_content,
            ai_response=ai_response,
            work_item_id=work_item_id,
            agent_id=agent.id,
            increment_version=enforce_strict_version,
        )
        filter_doc = self._build_update_filter(
            document_id=document_id,
            source_version=source_version,
            enforce_strict_version=enforce_strict_version,
        )
        if enforce_document_hash and source_document_hash:
            if not await self._matches_source_hash(
                collection=collection,
                document_id=document_id,
                expected_hash=source_document_hash,
            ):
                get_metrics_collector().record_hash_conflict(agent.id)
                logger.warning(
                    "Strict consistency blocked hash-mismatched write",
                    agent_id=agent.id,
                    document_id=document_id,
                )
                return False, "hash_conflict"

        try:
            # Perform update
            result = await collection.update_one(
                filter_doc,
                update,
            )

            if result.matched_count == 0:
                if enforce_strict_version:
                    get_metrics_collector().record_version_conflict(agent.id)
                    logger.warning(
                        "Strict consistency blocked stale write",
                        agent_id=agent.id,
                        document_id=document_id,
                        source_version=source_version,
                    )
                    return False, "strict_version_conflict"
                logger.warning(
                    "Document not found for update",
                    agent_id=agent.id,
                    document_id=document_id,
                )
                return False, "document_not_found"

            # Record idempotency key
            if idempotency_key:
                await self._record_idempotency(idempotency_key, agent.id, work_item_id)

            logger.info(
                "Wrote AI result",
                agent_id=agent.id,
                document_id=document_id,
                modified=result.modified_count > 0,
            )

            return True, "written"

        except DuplicateKeyError as e:
            raise WriteConflictError(target_coll, document_id)

        except Exception as e:
            logger.error(
                "Failed to write result",
                agent_id=agent.id,
                document_id=document_id,
                error=str(e),
            )
            raise

    async def _matches_source_hash(
        self,
        collection: AsyncIOMotorCollection[dict[str, Any]],
        document_id: str,
        expected_hash: str,
    ) -> bool:
        """Validate current document hash against dispatch-time hash."""
        current = await collection.find_one(
            {"_id": self._parse_document_id(document_id)}
        )
        if current is None:
            return False
        current_hash = self._stable_document_hash(current)
        return current_hash == expected_hash

    def _build_update(
        self,
        write_config: WriteConfig,
        parsed_content: dict[str, Any],
        ai_response: AIResponse,
        work_item_id: str,
        agent_id: str | None = None,
        increment_version: bool = False,
    ) -> dict[str, Any]:
        """Build the MongoDB update document."""
        # Map fields if configured
        if write_config.fields:
            mapped_content = {}
            for source_field, target_field in write_config.fields.items():
                if source_field in parsed_content:
                    mapped_content[target_field] = parsed_content[source_field]
            content = mapped_content
        else:
            content = parsed_content

        # Optionally nest output under a single target field.
        if write_config.target_field:
            content = {write_config.target_field: content}

        # Build update based on strategy
        update = self._strategy_handler.build_update(
            strategy=write_config.strategy,
            content=content,
            path=write_config.path,
            array_field=write_config.array_field,
        )

        # Add metadata if configured
        if write_config.include_metadata:
            metadata = {
                "processed_at": datetime.utcnow(),
                "work_item_id": work_item_id,
                "source_work_item_id": work_item_id,
                "source_agent_id": agent_id,
                "model": ai_response.model,
                "provider": ai_response.provider,
                "tokens": ai_response.total_tokens,
                "cost_usd": ai_response.cost_usd,
                "latency_ms": ai_response.latency_ms,
            }

            metadata_field = write_config.metadata_field
            if "$set" in update:
                update["$set"][metadata_field] = metadata
            else:
                update["$set"] = {metadata_field: metadata}

        if increment_version:
            update.setdefault("$inc", {})
            update["$inc"]["_mongoclaw_version"] = 1

        return update

    def _build_update_filter(
        self,
        document_id: str,
        source_version: int | None,
        enforce_strict_version: bool,
    ) -> dict[str, Any]:
        """Build update filter with optional strict version guard."""
        base_filter: dict[str, Any] = {"_id": self._parse_document_id(document_id)}
        if not enforce_strict_version:
            return base_filter

        expected_version = 0 if source_version is None else source_version
        if expected_version == 0:
            base_filter["$or"] = [
                {"_mongoclaw_version": 0},
                {"_mongoclaw_version": {"$exists": False}},
            ]
        else:
            base_filter["_mongoclaw_version"] = expected_version
        return base_filter

    def _parse_document_id(self, document_id: str) -> Any:
        """Parse document ID to appropriate type."""
        from bson import ObjectId

        # Try ObjectId first
        if len(document_id) == 24:
            try:
                return ObjectId(document_id)
            except Exception:
                pass

        return document_id

    def _stable_document_hash(self, document: dict[str, Any]) -> str:
        """Compute stable hash excluding mutable framework metadata."""
        normalized = self._normalize_for_hash(document)
        serialized = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode()).hexdigest()

    def _normalize_for_hash(self, value: Any) -> Any:
        """Normalize value recursively to a hash-stable representation."""
        if isinstance(value, dict):
            ignored = {"_ai_metadata", "_mongoclaw_version"}
            return {
                k: self._normalize_for_hash(v)
                for k, v in value.items()
                if k not in ignored
            }
        if isinstance(value, list):
            return [self._normalize_for_hash(v) for v in value]
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)

    async def _check_idempotency(self, key: str) -> bool:
        """Check if an idempotency key exists."""
        doc = await self.idempotency_collection.find_one({"key": key})
        return doc is not None

    async def _record_idempotency(
        self,
        key: str,
        agent_id: str,
        work_item_id: str,
    ) -> None:
        """Record an idempotency key."""
        try:
            await self.idempotency_collection.insert_one({
                "key": key,
                "agent_id": agent_id,
                "work_item_id": work_item_id,
                "created_at": datetime.utcnow(),
            })
        except DuplicateKeyError:
            # Already recorded (race condition), that's fine
            pass

    async def get_idempotency_stats(self) -> dict[str, Any]:
        """Get idempotency tracking statistics."""
        count = await self.idempotency_collection.count_documents({})
        return {
            "tracked_keys": count,
            "ttl_seconds": self._idempotency_ttl,
        }
