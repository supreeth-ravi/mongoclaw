"""Response parsing and JSON extraction from AI responses."""

from __future__ import annotations

import json
import re
from typing import Any

from mongoclaw.core.exceptions import AIResponseParseError
from mongoclaw.core.types import AIResponse
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)


class ResponseParser:
    """
    Parses AI responses and extracts structured data.

    Features:
    - JSON extraction from markdown code blocks
    - Schema validation
    - Fallback extraction strategies
    """

    # Patterns for JSON extraction
    JSON_BLOCK_PATTERN = re.compile(
        r"```(?:json)?\s*\n?(.*?)\n?```",
        re.DOTALL | re.IGNORECASE,
    )
    JSON_OBJECT_PATTERN = re.compile(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", re.DOTALL)
    JSON_ARRAY_PATTERN = re.compile(r"\[[^\[\]]*(?:\[[^\[\]]*\][^\[\]]*)*\]", re.DOTALL)

    def __init__(
        self,
        strict: bool = False,
        allow_partial: bool = True,
    ) -> None:
        self._strict = strict
        self._allow_partial = allow_partial

    def parse(
        self,
        response: AIResponse,
        schema: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Parse an AI response and extract structured data.

        Args:
            response: The AI response.
            schema: Optional JSON schema for validation.

        Returns:
            Parsed data as a dictionary.

        Raises:
            AIResponseParseError: If parsing fails.
        """
        content = response.content.strip()

        if not content:
            raise AIResponseParseError(
                "Empty response content",
                raw_response=content,
                provider=response.provider,
                model=response.model,
            )

        # Try to extract JSON
        parsed = self._extract_json(content)

        if parsed is None:
            if self._strict:
                raise AIResponseParseError(
                    "Could not extract JSON from response",
                    raw_response=content,
                    provider=response.provider,
                    model=response.model,
                )
            # Return raw content as fallback
            return {"content": content, "_raw": True}

        # Validate against schema if provided
        if schema:
            errors = self._validate_schema(parsed, schema)
            if errors:
                if self._strict:
                    raise AIResponseParseError(
                        f"Schema validation failed: {'; '.join(errors)}",
                        raw_response=content,
                        provider=response.provider,
                        model=response.model,
                    )
                logger.warning(
                    "Schema validation warnings",
                    errors=errors,
                )

        # Update response with parsed content
        response.parsed_content = parsed

        return parsed

    def _extract_json(self, content: str) -> dict[str, Any] | list[Any] | None:
        """
        Extract JSON from content using multiple strategies.

        Args:
            content: The content to parse.

        Returns:
            Extracted JSON or None.
        """
        # Strategy 1: Try parsing entire content as JSON
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass

        # Strategy 2: Extract from markdown code block
        match = self.JSON_BLOCK_PATTERN.search(content)
        if match:
            try:
                return json.loads(match.group(1).strip())
            except json.JSONDecodeError:
                pass

        # Strategy 3: Find JSON object in content
        match = self.JSON_OBJECT_PATTERN.search(content)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

        # Strategy 4: Find JSON array in content
        match = self.JSON_ARRAY_PATTERN.search(content)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

        # Strategy 5: Try to fix common JSON issues
        fixed = self._fix_json(content)
        if fixed:
            try:
                return json.loads(fixed)
            except json.JSONDecodeError:
                pass

        return None

    def _fix_json(self, content: str) -> str | None:
        """
        Attempt to fix common JSON formatting issues.

        Args:
            content: The content to fix.

        Returns:
            Fixed content or None.
        """
        # Remove trailing commas
        fixed = re.sub(r",\s*([}\]])", r"\1", content)

        # Add missing quotes to keys
        fixed = re.sub(r"(\{|,)\s*(\w+)\s*:", r'\1 "\2":', fixed)

        # Replace single quotes with double quotes
        fixed = fixed.replace("'", '"')

        # Find JSON boundaries
        start = fixed.find("{")
        end = fixed.rfind("}") + 1

        if start >= 0 and end > start:
            return fixed[start:end]

        start = fixed.find("[")
        end = fixed.rfind("]") + 1

        if start >= 0 and end > start:
            return fixed[start:end]

        return None

    def _validate_schema(
        self,
        data: Any,
        schema: dict[str, Any],
    ) -> list[str]:
        """
        Validate data against a JSON schema.

        This is a simplified validator supporting common patterns.

        Args:
            data: The data to validate.
            schema: The JSON schema.

        Returns:
            List of validation errors.
        """
        errors: list[str] = []

        schema_type = schema.get("type")

        # Type validation
        if schema_type:
            if not self._check_type(data, schema_type):
                errors.append(f"Expected type '{schema_type}', got '{type(data).__name__}'")
                return errors

        # Object validation
        if schema_type == "object" and isinstance(data, dict):
            properties = schema.get("properties", {})
            required = schema.get("required", [])

            # Check required fields
            for field in required:
                if field not in data:
                    errors.append(f"Missing required field: '{field}'")

            # Validate property types
            for field, field_schema in properties.items():
                if field in data:
                    field_errors = self._validate_schema(data[field], field_schema)
                    errors.extend([f"{field}.{e}" for e in field_errors])

        # Array validation
        if schema_type == "array" and isinstance(data, list):
            items_schema = schema.get("items")
            if items_schema:
                for i, item in enumerate(data):
                    item_errors = self._validate_schema(item, items_schema)
                    errors.extend([f"[{i}].{e}" for e in item_errors])

        # Enum validation
        if "enum" in schema:
            if data not in schema["enum"]:
                errors.append(f"Value must be one of: {schema['enum']}")

        return errors

    def _check_type(self, value: Any, expected_type: str) -> bool:
        """Check if a value matches the expected JSON schema type."""
        type_map = {
            "string": str,
            "number": (int, float),
            "integer": int,
            "boolean": bool,
            "array": list,
            "object": dict,
            "null": type(None),
        }

        expected = type_map.get(expected_type)
        if expected is None:
            return True

        return isinstance(value, expected)

    def extract_field(
        self,
        response: AIResponse,
        field: str,
        default: Any = None,
    ) -> Any:
        """
        Extract a specific field from a parsed response.

        Args:
            response: The AI response.
            field: The field path (supports dot notation).
            default: Default value if not found.

        Returns:
            The field value or default.
        """
        parsed = response.parsed_content
        if parsed is None:
            try:
                parsed = self.parse(response)
            except AIResponseParseError:
                return default

        return self._get_nested(parsed, field, default)

    def _get_nested(
        self,
        data: dict[str, Any],
        path: str,
        default: Any = None,
    ) -> Any:
        """Get a nested value using dot notation."""
        if not isinstance(data, dict):
            return default

        parts = path.split(".")
        current = data

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                if 0 <= idx < len(current):
                    current = current[idx]
                else:
                    return default
            else:
                return default

        return current if current is not None else default
