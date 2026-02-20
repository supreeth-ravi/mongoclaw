"""Idempotency key generation utilities."""

from __future__ import annotations

import hashlib
from typing import Any

from jinja2 import Environment

from mongoclaw.agents.models import AgentConfig
from mongoclaw.dispatcher.work_item import WorkItem


class IdempotencyKeyGenerator:
    """
    Generates idempotency keys for deduplication.

    Keys can be generated using:
    - Automatic content-based hashing
    - Custom Jinja2 templates
    - Composite keys from multiple fields
    """

    def __init__(self, jinja_env: Environment | None = None) -> None:
        self._jinja_env = jinja_env or Environment()

    def generate(
        self,
        agent: AgentConfig,
        work_item: WorkItem,
    ) -> str:
        """
        Generate an idempotency key for a work item.

        Args:
            agent: The agent configuration.
            work_item: The work item.

        Returns:
            Idempotency key string.
        """
        template = agent.write.idempotency_key

        if template:
            return self._render_template(template, work_item)

        return self._generate_default(agent.id, work_item)

    def _render_template(self, template: str, work_item: WorkItem) -> str:
        """Render an idempotency key template."""
        try:
            compiled = self._jinja_env.from_string(template)
            context = {
                "agent_id": work_item.agent_id,
                "document": work_item.document,
                "document_id": work_item.document_id,
                "database": work_item.database,
                "collection": work_item.collection,
                "event": work_item.change_event,
            }
            return compiled.render(**context)
        except Exception:
            # Fall back to default
            return self._generate_default(work_item.agent_id, work_item)

    def _generate_default(self, agent_id: str, work_item: WorkItem) -> str:
        """Generate a default idempotency key."""
        # Create a hash of the document content
        doc_hash = self._hash_document(work_item.document)

        return f"{agent_id}:{work_item.document_id}:{doc_hash}"

    def _hash_document(self, document: dict[str, Any]) -> str:
        """Create a stable hash of a document."""
        # Remove volatile fields
        stable_doc = self._remove_volatile_fields(document)

        # Sort keys for stable serialization
        import json
        serialized = json.dumps(stable_doc, sort_keys=True, default=str)

        return hashlib.md5(serialized.encode()).hexdigest()[:12]

    def _remove_volatile_fields(self, document: dict[str, Any]) -> dict[str, Any]:
        """Remove fields that shouldn't affect idempotency."""
        volatile_prefixes = ("_ai_", "_mongoclaw_", "updated_at", "modified_at")

        return {
            k: v
            for k, v in document.items()
            if not any(k.startswith(p) or k == p for p in volatile_prefixes)
        }

    @staticmethod
    def composite_key(*parts: str) -> str:
        """Create a composite key from multiple parts."""
        return ":".join(str(p) for p in parts)

    @staticmethod
    def hash_key(key: str) -> str:
        """Hash a key to a fixed-length string."""
        return hashlib.sha256(key.encode()).hexdigest()[:32]


class IdempotencyWindow:
    """
    Tracks idempotency within a time window.

    Uses in-memory tracking with TTL for quick duplicate detection
    before checking the database.
    """

    def __init__(self, window_seconds: int = 300) -> None:
        self._window_seconds = window_seconds
        self._seen: dict[str, float] = {}
        self._max_size = 10000

    def check(self, key: str) -> bool:
        """
        Check if a key was seen recently.

        Args:
            key: The idempotency key.

        Returns:
            True if key was seen in the window.
        """
        import time

        now = time.time()
        self._cleanup(now)

        return key in self._seen

    def record(self, key: str) -> None:
        """
        Record a key as seen.

        Args:
            key: The idempotency key.
        """
        import time

        now = time.time()

        # Evict oldest if at capacity
        if len(self._seen) >= self._max_size:
            self._evict_oldest()

        self._seen[key] = now

    def _cleanup(self, now: float) -> None:
        """Remove expired entries."""
        cutoff = now - self._window_seconds
        self._seen = {k: v for k, v in self._seen.items() if v > cutoff}

    def _evict_oldest(self) -> None:
        """Evict the oldest entry."""
        if self._seen:
            oldest = min(self._seen, key=lambda k: self._seen[k])
            del self._seen[oldest]

    def clear(self) -> None:
        """Clear all tracked keys."""
        self._seen.clear()
