"""Write strategy implementations."""

from __future__ import annotations

from typing import Any

from mongoclaw.core.types import WriteStrategy


class WriteStrategyHandler:
    """
    Handles different write strategies for MongoDB updates.

    Strategies:
    - MERGE: Merge fields into existing document
    - REPLACE: Replace specified fields
    - APPEND: Append to an array field
    - NESTED: Write to a nested path
    """

    def build_update(
        self,
        strategy: WriteStrategy,
        content: dict[str, Any],
        path: str | None = None,
        array_field: str | None = None,
    ) -> dict[str, Any]:
        """
        Build a MongoDB update document for the given strategy.

        Args:
            strategy: The write strategy.
            content: The content to write.
            path: Nested path (for NESTED strategy).
            array_field: Array field name (for APPEND strategy).

        Returns:
            MongoDB update document.
        """
        if strategy == WriteStrategy.MERGE:
            return self._build_merge_update(content)

        if strategy == WriteStrategy.REPLACE:
            return self._build_replace_update(content)

        if strategy == WriteStrategy.APPEND:
            return self._build_append_update(content, array_field)

        if strategy == WriteStrategy.NESTED:
            return self._build_nested_update(content, path)

        # Default to merge
        return self._build_merge_update(content)

    def _build_merge_update(self, content: dict[str, Any]) -> dict[str, Any]:
        """
        Build a merge update.

        Merges content fields into the existing document.
        Existing fields not in content are preserved.
        """
        return {"$set": content}

    def _build_replace_update(self, content: dict[str, Any]) -> dict[str, Any]:
        """
        Build a replace update.

        Same as merge for individual fields.
        Use when you want to explicitly replace values.
        """
        return {"$set": content}

    def _build_append_update(
        self,
        content: dict[str, Any],
        array_field: str | None,
    ) -> dict[str, Any]:
        """
        Build an append update.

        Appends content to an array field.
        """
        if not array_field:
            raise ValueError("array_field is required for APPEND strategy")

        return {
            "$push": {
                array_field: {
                    "$each": [content] if isinstance(content, dict) else content,
                }
            }
        }

    def _build_nested_update(
        self,
        content: dict[str, Any],
        path: str | None,
    ) -> dict[str, Any]:
        """
        Build a nested update.

        Writes content to a nested path in the document.
        """
        if not path:
            raise ValueError("path is required for NESTED strategy")

        # Build nested $set
        nested_set = {}
        for key, value in content.items():
            nested_set[f"{path}.{key}"] = value

        return {"$set": nested_set}

    def build_conditional_update(
        self,
        strategy: WriteStrategy,
        content: dict[str, Any],
        condition: dict[str, Any],
        path: str | None = None,
        array_field: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Build a conditional update with a filter.

        Args:
            strategy: The write strategy.
            content: The content to write.
            condition: Additional filter condition.
            path: Nested path (for NESTED strategy).
            array_field: Array field name (for APPEND strategy).

        Returns:
            Tuple of (filter, update) documents.
        """
        update = self.build_update(strategy, content, path, array_field)
        return condition, update


class ArrayAppendStrategy:
    """
    Specialized handler for array append operations.

    Supports:
    - Append single item
    - Append multiple items
    - Append with limit (capped arrays)
    - Append unique (no duplicates)
    """

    @staticmethod
    def append_one(
        array_field: str,
        item: Any,
    ) -> dict[str, Any]:
        """Append a single item."""
        return {"$push": {array_field: item}}

    @staticmethod
    def append_many(
        array_field: str,
        items: list[Any],
    ) -> dict[str, Any]:
        """Append multiple items."""
        return {"$push": {array_field: {"$each": items}}}

    @staticmethod
    def append_with_limit(
        array_field: str,
        items: list[Any],
        limit: int,
        position: int = 0,
    ) -> dict[str, Any]:
        """
        Append items with a size limit (capped array).

        Args:
            array_field: The array field name.
            items: Items to append.
            limit: Maximum array size.
            position: Position to insert (0 = beginning, -1 = end).

        Returns:
            MongoDB update document.
        """
        slice_val = limit if position == 0 else -limit

        return {
            "$push": {
                array_field: {
                    "$each": items,
                    "$position": position,
                    "$slice": slice_val,
                }
            }
        }

    @staticmethod
    def append_unique(
        array_field: str,
        items: list[Any],
    ) -> dict[str, Any]:
        """Append items only if they don't exist (set semantics)."""
        return {"$addToSet": {array_field: {"$each": items}}}


class NestedUpdateStrategy:
    """
    Specialized handler for nested document updates.

    Supports:
    - Deep path updates
    - Array element updates
    - Conditional nested updates
    """

    @staticmethod
    def set_at_path(path: str, value: Any) -> dict[str, Any]:
        """Set a value at a nested path."""
        return {"$set": {path: value}}

    @staticmethod
    def set_multiple_at_path(
        base_path: str,
        values: dict[str, Any],
    ) -> dict[str, Any]:
        """Set multiple values under a base path."""
        updates = {f"{base_path}.{k}": v for k, v in values.items()}
        return {"$set": updates}

    @staticmethod
    def update_array_element(
        array_field: str,
        element_filter: dict[str, Any],
        update_fields: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Update a specific array element.

        Args:
            array_field: The array field name.
            element_filter: Filter to find the element.
            update_fields: Fields to update on the element.

        Returns:
            Tuple of (filter, update) for use with update_one.
        """
        # Build filter with array element match
        filter_doc = {f"{array_field}": {"$elemMatch": element_filter}}

        # Build update using positional operator
        update = {
            "$set": {
                f"{array_field}.$.{k}": v
                for k, v in update_fields.items()
            }
        }

        return filter_doc, update

    @staticmethod
    def increment_at_path(path: str, amount: int | float = 1) -> dict[str, Any]:
        """Increment a numeric value at a path."""
        return {"$inc": {path: amount}}
