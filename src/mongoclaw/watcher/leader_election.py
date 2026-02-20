"""Leader election for distributed change stream watching."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Coroutine

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import IndexModel, ASCENDING
from pymongo.errors import DuplicateKeyError

from mongoclaw.core.exceptions import NotLeaderError
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)


class LeaderElection:
    """
    Distributed leader election using MongoDB.

    Only one instance can be the leader at a time. The leader is responsible
    for running change stream watchers to avoid duplicate event processing.

    Uses a lease-based approach with heartbeats:
    - Leader acquires a lease by inserting/updating a lock document
    - Leader must renew lease before TTL expires
    - If leader fails to renew, another instance can take over
    """

    DEFAULT_LEASE_DURATION = timedelta(seconds=30)
    DEFAULT_RENEW_INTERVAL = timedelta(seconds=10)

    def __init__(
        self,
        client: AsyncIOMotorClient[dict[str, Any]],
        database: str = "mongoclaw",
        collection: str = "leader_election",
        lock_name: str = "change_stream_leader",
        lease_duration: timedelta | None = None,
        renew_interval: timedelta | None = None,
        instance_id: str | None = None,
    ) -> None:
        self._client = client
        self._database_name = database
        self._collection_name = collection
        self._lock_name = lock_name

        self._lease_duration = lease_duration or self.DEFAULT_LEASE_DURATION
        self._renew_interval = renew_interval or self.DEFAULT_RENEW_INTERVAL

        # Generate unique instance ID
        hostname = os.environ.get("HOSTNAME", "unknown")
        self._instance_id = instance_id or f"{hostname}-{uuid.uuid4().hex[:8]}"

        self._is_leader = False
        self._renew_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._collection: AsyncIOMotorCollection[dict[str, Any]] | None = None

        # Callbacks
        self._on_elected: Callable[[], Coroutine[Any, Any, None]] | None = None
        self._on_demoted: Callable[[], Coroutine[Any, Any, None]] | None = None

    @property
    def collection(self) -> AsyncIOMotorCollection[dict[str, Any]]:
        """Get collection instance."""
        if self._collection is None:
            self._collection = self._client[self._database_name][self._collection_name]
        return self._collection

    @property
    def instance_id(self) -> str:
        """Get this instance's ID."""
        return self._instance_id

    @property
    def is_leader(self) -> bool:
        """Check if this instance is the leader."""
        return self._is_leader

    def on_elected(
        self, callback: Callable[[], Coroutine[Any, Any, None]]
    ) -> None:
        """Set callback for when this instance becomes leader."""
        self._on_elected = callback

    def on_demoted(
        self, callback: Callable[[], Coroutine[Any, Any, None]]
    ) -> None:
        """Set callback for when this instance loses leadership."""
        self._on_demoted = callback

    async def initialize(self) -> None:
        """Initialize indexes and collection setup."""
        # Create TTL index for automatic cleanup of stale locks
        indexes = [
            IndexModel(
                [("lock_name", ASCENDING)],
                unique=True,
            ),
            IndexModel(
                [("expires_at", ASCENDING)],
                expireAfterSeconds=0,  # TTL index
            ),
        ]
        await self.collection.create_indexes(indexes)
        logger.debug("Leader election initialized", instance_id=self._instance_id)

    async def start(self) -> None:
        """Start participating in leader election."""
        logger.info("Starting leader election", instance_id=self._instance_id)

        self._stop_event.clear()

        # Try to acquire leadership immediately
        await self._try_acquire()

        # Start renewal/acquisition loop
        self._renew_task = asyncio.create_task(
            self._election_loop(),
            name=f"leader_election_{self._instance_id}",
        )

    async def stop(self) -> None:
        """Stop participating in leader election."""
        logger.info("Stopping leader election", instance_id=self._instance_id)

        self._stop_event.set()

        if self._renew_task:
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass
            self._renew_task = None

        # Release leadership if we have it
        if self._is_leader:
            await self._release()

    async def _election_loop(self) -> None:
        """Main election loop."""
        while not self._stop_event.is_set():
            try:
                if self._is_leader:
                    # Try to renew leadership
                    success = await self._renew()
                    if not success:
                        await self._handle_lost_leadership()
                else:
                    # Try to acquire leadership
                    await self._try_acquire()

                # Wait for next check
                await asyncio.sleep(self._renew_interval.total_seconds())

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(
                    "Error in leader election loop",
                    error=str(e),
                    instance_id=self._instance_id,
                )
                await asyncio.sleep(5)  # Brief pause on error

    async def _try_acquire(self) -> bool:
        """
        Try to acquire leadership.

        Returns:
            True if leadership was acquired.
        """
        now = datetime.utcnow()
        expires_at = now + self._lease_duration

        try:
            # Try to insert new lock or update expired one
            result = await self.collection.update_one(
                {
                    "lock_name": self._lock_name,
                    "$or": [
                        {"holder": self._instance_id},  # Already ours
                        {"expires_at": {"$lt": now}},  # Expired
                    ],
                },
                {
                    "$set": {
                        "holder": self._instance_id,
                        "expires_at": expires_at,
                        "acquired_at": now,
                    },
                    "$setOnInsert": {
                        "lock_name": self._lock_name,
                    },
                },
                upsert=True,
            )

            if result.modified_count > 0 or result.upserted_id:
                if not self._is_leader:
                    self._is_leader = True
                    logger.info(
                        "Acquired leadership",
                        instance_id=self._instance_id,
                    )
                    if self._on_elected:
                        await self._on_elected()
                return True

        except DuplicateKeyError:
            # Another instance holds the lock
            pass
        except Exception as e:
            logger.warning(
                "Failed to acquire leadership",
                error=str(e),
                instance_id=self._instance_id,
            )

        return False

    async def _renew(self) -> bool:
        """
        Renew the leadership lease.

        Returns:
            True if lease was renewed.
        """
        now = datetime.utcnow()
        expires_at = now + self._lease_duration

        result = await self.collection.update_one(
            {
                "lock_name": self._lock_name,
                "holder": self._instance_id,
            },
            {
                "$set": {
                    "expires_at": expires_at,
                    "renewed_at": now,
                }
            },
        )

        if result.modified_count > 0:
            logger.debug(
                "Renewed leadership lease",
                instance_id=self._instance_id,
                expires_at=expires_at.isoformat(),
            )
            return True

        return False

    async def _release(self) -> None:
        """Release leadership."""
        result = await self.collection.delete_one(
            {
                "lock_name": self._lock_name,
                "holder": self._instance_id,
            }
        )

        if result.deleted_count > 0:
            logger.info(
                "Released leadership",
                instance_id=self._instance_id,
            )

        await self._handle_lost_leadership()

    async def _handle_lost_leadership(self) -> None:
        """Handle losing leadership."""
        if self._is_leader:
            self._is_leader = False
            logger.warning(
                "Lost leadership",
                instance_id=self._instance_id,
            )
            if self._on_demoted:
                await self._on_demoted()

    async def get_current_leader(self) -> str | None:
        """
        Get the current leader's instance ID.

        Returns:
            Leader instance ID or None if no leader.
        """
        doc = await self.collection.find_one(
            {
                "lock_name": self._lock_name,
                "expires_at": {"$gt": datetime.utcnow()},
            }
        )

        if doc:
            return doc.get("holder")
        return None

    def require_leader(self) -> None:
        """
        Assert that this instance is the leader.

        Raises:
            NotLeaderError: If this instance is not the leader.
        """
        if not self._is_leader:
            raise NotLeaderError(
                instance_id=self._instance_id,
                leader_id="unknown",
            )
