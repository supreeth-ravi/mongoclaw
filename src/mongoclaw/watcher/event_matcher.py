"""Event matching logic for routing change events to agents."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mongoclaw.core.types import ChangeEvent, ChangeOperation
from mongoclaw.observability.logging import get_logger

if TYPE_CHECKING:
    from mongoclaw.agents.models import AgentConfig
    from mongoclaw.agents.store import AgentStore

logger = get_logger(__name__)


class EventMatcher:
    """
    Matches change events to agent configurations.

    Evaluates:
    - Database and collection match
    - Operation type match
    - Document filter match
    """

    def __init__(self, agent_store: AgentStore) -> None:
        self._agent_store = agent_store
        self._cache: dict[str, list[AgentConfig]] = {}
        self._cache_version: int = 0

    async def match(self, event: ChangeEvent) -> list[AgentConfig]:
        """
        Find all agents that match a change event.

        Args:
            event: The change event to match.

        Returns:
            List of matching agent configurations.
        """
        # Get agents watching this namespace
        agents = await self._agent_store.get_by_watch_target(
            database=event.database,
            collection=event.collection,
            enabled_only=True,
        )

        if not agents:
            return []

        matched: list[AgentConfig] = []

        for agent in agents:
            if self._matches_agent(event, agent):
                matched.append(agent)

        return matched

    def _matches_agent(self, event: ChangeEvent, agent: AgentConfig) -> bool:
        """
        Check if an event matches an agent's configuration.

        Args:
            event: The change event.
            agent: The agent configuration.

        Returns:
            True if the event matches.
        """
        watch = agent.watch

        # Check operation type
        if event.operation not in watch.operations:
            logger.debug(
                "Operation mismatch",
                agent_id=agent.id,
                event_op=event.operation.value,
                watch_ops=[op.value for op in watch.operations],
            )
            return False

        # Check document filter
        if watch.filter and event.full_document:
            if not self._matches_filter(event.full_document, watch.filter):
                logger.debug(
                    "Filter mismatch",
                    agent_id=agent.id,
                    document_id=event.document_id,
                )
                return False

        # For delete operations without full document, skip filter check
        if event.operation == ChangeOperation.DELETE and not event.full_document:
            if watch.filter:
                logger.debug(
                    "Skipping delete event with filter (no full document)",
                    agent_id=agent.id,
                    document_id=event.document_id,
                )
                return False

        return True

    def _matches_filter(
        self,
        document: dict[str, Any],
        filter_doc: dict[str, Any],
    ) -> bool:
        """
        Check if a document matches a filter.

        This is a simplified filter matcher supporting basic operators.
        For complex filters, a full MongoDB query engine would be needed.

        Args:
            document: The document to check.
            filter_doc: The filter specification.

        Returns:
            True if the document matches the filter.
        """
        for key, value in filter_doc.items():
            # Handle operators
            if key.startswith("$"):
                if not self._evaluate_operator(key, value, document):
                    return False
            else:
                # Field match
                if not self._matches_field(document, key, value):
                    return False

        return True

    def _matches_field(
        self,
        document: dict[str, Any],
        field: str,
        expected: Any,
    ) -> bool:
        """
        Check if a document field matches an expected value.

        Args:
            document: The document.
            field: The field path (supports dot notation).
            expected: The expected value or operator.

        Returns:
            True if the field matches.
        """
        # Get field value with dot notation support
        actual = self._get_field_value(document, field)

        # Handle operator expressions
        if isinstance(expected, dict):
            return self._evaluate_field_operators(actual, expected)

        # Direct comparison
        return actual == expected

    def _get_field_value(self, document: dict[str, Any], field: str) -> Any:
        """
        Get a field value from a document with dot notation support.

        Args:
            document: The document.
            field: The field path.

        Returns:
            The field value or None if not found.
        """
        parts = field.split(".")
        current = document

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                if 0 <= idx < len(current):
                    current = current[idx]
                else:
                    return None
            else:
                return None

        return current

    def _evaluate_field_operators(
        self,
        actual: Any,
        operators: dict[str, Any],
    ) -> bool:
        """
        Evaluate field-level operators.

        Args:
            actual: The actual field value.
            operators: Dictionary of operators to evaluate.

        Returns:
            True if all operators match.
        """
        for op, value in operators.items():
            if not self._evaluate_comparison_operator(op, actual, value):
                return False
        return True

    def _evaluate_comparison_operator(
        self,
        op: str,
        actual: Any,
        expected: Any,
    ) -> bool:
        """
        Evaluate a comparison operator.

        Args:
            op: The operator (e.g., "$eq", "$gt").
            actual: The actual value.
            expected: The expected value.

        Returns:
            True if the comparison passes.
        """
        if op == "$eq":
            return actual == expected

        if op == "$ne":
            return actual != expected

        if op == "$gt":
            return actual is not None and actual > expected

        if op == "$gte":
            return actual is not None and actual >= expected

        if op == "$lt":
            return actual is not None and actual < expected

        if op == "$lte":
            return actual is not None and actual <= expected

        if op == "$in":
            return actual in expected

        if op == "$nin":
            return actual not in expected

        if op == "$exists":
            return (actual is not None) == expected

        if op == "$type":
            return self._check_type(actual, expected)

        if op == "$regex":
            import re
            if actual is None:
                return False
            pattern = expected
            flags = 0
            if isinstance(expected, dict):
                pattern = expected.get("$regex", "")
                options = expected.get("$options", "")
                if "i" in options:
                    flags |= re.IGNORECASE
            return bool(re.search(pattern, str(actual), flags))

        # Unknown operator - log and skip
        logger.warning("Unknown operator", operator=op)
        return True

    def _evaluate_operator(
        self,
        op: str,
        value: Any,
        document: dict[str, Any],
    ) -> bool:
        """
        Evaluate a top-level logical operator.

        Args:
            op: The operator (e.g., "$and", "$or").
            value: The operator value.
            document: The document to check.

        Returns:
            True if the operator evaluates to true.
        """
        if op == "$and":
            return all(
                self._matches_filter(document, clause)
                for clause in value
            )

        if op == "$or":
            return any(
                self._matches_filter(document, clause)
                for clause in value
            )

        if op == "$not":
            return not self._matches_filter(document, value)

        if op == "$nor":
            return not any(
                self._matches_filter(document, clause)
                for clause in value
            )

        # Unknown operator
        logger.warning("Unknown top-level operator", operator=op)
        return True

    def _check_type(self, value: Any, expected_type: str | int) -> bool:
        """
        Check if a value matches a BSON type.

        Args:
            value: The value to check.
            expected_type: The expected type name or number.

        Returns:
            True if the type matches.
        """
        type_map = {
            "double": (float,),
            "string": (str,),
            "object": (dict,),
            "array": (list,),
            "bool": (bool,),
            "int": (int,),
            "long": (int,),
            "null": (type(None),),
            1: (float,),
            2: (str,),
            3: (dict,),
            4: (list,),
            8: (bool,),
            16: (int,),
            18: (int,),
            10: (type(None),),
        }

        expected_types = type_map.get(expected_type, ())
        return isinstance(value, expected_types)

    def invalidate_cache(self) -> None:
        """Invalidate the agent cache."""
        self._cache.clear()
        self._cache_version += 1
