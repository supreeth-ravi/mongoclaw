"""Routing strategies for work item distribution."""

from __future__ import annotations

import hashlib
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mongoclaw.agents.models import AgentConfig
    from mongoclaw.dispatcher.work_item import WorkItem


class RoutingStrategy(str, Enum):
    """Strategies for routing work items to streams."""

    # One stream per agent
    BY_AGENT = "by_agent"

    # One stream per collection
    BY_COLLECTION = "by_collection"

    # Single global stream
    SINGLE = "single"

    # Partition by document ID hash
    PARTITIONED = "partitioned"

    # Route by priority
    BY_PRIORITY = "by_priority"


# Default stream names
DEFAULT_STREAM = "mongoclaw:work"
DLQ_STREAM = "mongoclaw:dlq"


def get_stream_name(
    agent: AgentConfig,
    work_item: WorkItem,
    strategy: RoutingStrategy = RoutingStrategy.BY_AGENT,
    num_partitions: int = 8,
) -> str:
    """
    Determine the stream name for a work item.

    Args:
        agent: The agent configuration.
        work_item: The work item.
        strategy: The routing strategy.
        num_partitions: Number of partitions (for PARTITIONED strategy).

    Returns:
        The stream name.
    """
    if strategy == RoutingStrategy.BY_AGENT:
        return f"mongoclaw:agent:{agent.id}"

    if strategy == RoutingStrategy.BY_COLLECTION:
        return f"mongoclaw:collection:{work_item.database}:{work_item.collection}"

    if strategy == RoutingStrategy.SINGLE:
        return DEFAULT_STREAM

    if strategy == RoutingStrategy.PARTITIONED:
        partition = _hash_partition(work_item.document_id, num_partitions)
        return f"mongoclaw:partition:{partition}"

    if strategy == RoutingStrategy.BY_PRIORITY:
        return f"mongoclaw:priority:{work_item.priority}"

    # Default fallback
    return DEFAULT_STREAM


def get_dlq_stream_name(
    agent: AgentConfig | None = None,
    strategy: RoutingStrategy = RoutingStrategy.BY_AGENT,
) -> str:
    """
    Get the dead letter queue stream name.

    Args:
        agent: Optional agent configuration.
        strategy: The routing strategy.

    Returns:
        The DLQ stream name.
    """
    if strategy == RoutingStrategy.BY_AGENT and agent:
        return f"mongoclaw:dlq:agent:{agent.id}"

    return DLQ_STREAM


def _hash_partition(key: str, num_partitions: int) -> int:
    """
    Hash a key to a partition number.

    Args:
        key: The key to hash.
        num_partitions: Number of partitions.

    Returns:
        Partition number (0 to num_partitions-1).
    """
    if not key:
        return 0

    hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
    return hash_val % num_partitions


def get_all_stream_patterns(
    strategy: RoutingStrategy = RoutingStrategy.BY_AGENT,
) -> list[str]:
    """
    Get stream name patterns for a routing strategy.

    Args:
        strategy: The routing strategy.

    Returns:
        List of stream name patterns (for scanning).
    """
    patterns = {
        RoutingStrategy.BY_AGENT: ["mongoclaw:agent:*"],
        RoutingStrategy.BY_COLLECTION: ["mongoclaw:collection:*"],
        RoutingStrategy.SINGLE: [DEFAULT_STREAM],
        RoutingStrategy.PARTITIONED: ["mongoclaw:partition:*"],
        RoutingStrategy.BY_PRIORITY: ["mongoclaw:priority:*"],
    }

    return patterns.get(strategy, [DEFAULT_STREAM])
