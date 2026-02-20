"""Jinja2 template engine for prompt rendering."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from jinja2 import (
    BaseLoader,
    Environment,
    StrictUndefined,
    TemplateError,
    TemplateSyntaxError,
    UndefinedError,
)

from mongoclaw.core.exceptions import PromptRenderError
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)


class PromptEngine:
    """
    Jinja2-based prompt template engine.

    Features:
    - Variable interpolation from document context
    - Custom filters for data transformation
    - Strict undefined variable handling
    - Template caching
    """

    def __init__(
        self,
        strict: bool = True,
        cache_size: int = 100,
    ) -> None:
        self._env = Environment(
            loader=BaseLoader(),
            undefined=StrictUndefined if strict else None,
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Register custom filters
        self._register_filters()

        # Template cache
        self._cache: dict[str, Any] = {}
        self._cache_size = cache_size

    def _register_filters(self) -> None:
        """Register custom Jinja2 filters."""
        self._env.filters.update({
            "json": self._filter_json,
            "truncate_words": self._filter_truncate_words,
            "default_if_none": self._filter_default_if_none,
            "format_date": self._filter_format_date,
            "extract_field": self._filter_extract_field,
            "list_to_text": self._filter_list_to_text,
            "sanitize": self._filter_sanitize,
            "first_n": self._filter_first_n,
            "keys": self._filter_keys,
            "values": self._filter_values,
        })

    def render(
        self,
        template: str,
        context: dict[str, Any],
        template_name: str | None = None,
    ) -> str:
        """
        Render a prompt template with the given context.

        Args:
            template: The Jinja2 template string.
            context: Variables available in the template.
            template_name: Optional name for error messages.

        Returns:
            The rendered prompt string.

        Raises:
            PromptRenderError: If rendering fails.
        """
        try:
            # Get or compile template
            compiled = self._get_compiled_template(template)

            # Render with context
            result = compiled.render(**context)

            # Clean up whitespace
            result = self._clean_whitespace(result)

            return result

        except UndefinedError as e:
            raise PromptRenderError(
                f"Undefined variable: {e}",
                template_name=template_name,
            )

        except TemplateSyntaxError as e:
            raise PromptRenderError(
                f"Template syntax error: {e.message}",
                template_name=template_name,
            )

        except TemplateError as e:
            raise PromptRenderError(
                f"Template error: {e}",
                template_name=template_name,
            )

        except Exception as e:
            raise PromptRenderError(
                f"Unexpected error: {e}",
                template_name=template_name,
            )

    def _get_compiled_template(self, template: str) -> Any:
        """Get a compiled template, using cache if available."""
        template_hash = hash(template)

        if template_hash in self._cache:
            return self._cache[template_hash]

        # Compile template
        compiled = self._env.from_string(template)

        # Cache with LRU-style eviction
        if len(self._cache) >= self._cache_size:
            # Remove oldest entry
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]

        self._cache[template_hash] = compiled
        return compiled

    def _clean_whitespace(self, text: str) -> str:
        """Clean up excessive whitespace."""
        lines = text.split("\n")
        # Remove leading/trailing empty lines
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        return "\n".join(lines)

    def build_context(
        self,
        document: dict[str, Any],
        change_event: dict[str, Any] | None = None,
        agent_config: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Build a context dictionary for template rendering.

        Args:
            document: The MongoDB document.
            change_event: Optional change event data.
            agent_config: Optional agent configuration.
            extra: Optional extra variables.

        Returns:
            Context dictionary for template rendering.
        """
        context: dict[str, Any] = {
            # Primary document access
            "document": document,
            "doc": document,  # Shorthand alias

            # Metadata
            "now": datetime.utcnow(),
            "timestamp": datetime.utcnow().isoformat(),
        }

        if change_event:
            context["event"] = change_event
            context["operation"] = change_event.get("operation")

        if agent_config:
            context["agent"] = agent_config

        if extra:
            context.update(extra)

        return context

    # Custom filters

    @staticmethod
    def _filter_json(value: Any, indent: int | None = None) -> str:
        """Convert value to JSON string."""
        return json.dumps(value, indent=indent, default=str)

    @staticmethod
    def _filter_truncate_words(value: str, length: int, suffix: str = "...") -> str:
        """Truncate text to a number of words."""
        if not value:
            return ""
        words = value.split()
        if len(words) <= length:
            return value
        return " ".join(words[:length]) + suffix

    @staticmethod
    def _filter_default_if_none(value: Any, default: Any = "") -> Any:
        """Return default if value is None."""
        return default if value is None else value

    @staticmethod
    def _filter_format_date(value: Any, fmt: str = "%Y-%m-%d") -> str:
        """Format a datetime value."""
        if value is None:
            return ""
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                return value
        if isinstance(value, datetime):
            return value.strftime(fmt)
        return str(value)

    @staticmethod
    def _filter_extract_field(value: dict[str, Any], field: str) -> Any:
        """Extract a field from a dictionary with dot notation."""
        if not isinstance(value, dict):
            return None
        parts = field.split(".")
        current = value
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current

    @staticmethod
    def _filter_list_to_text(
        value: list[Any],
        separator: str = ", ",
        last_separator: str | None = None,
    ) -> str:
        """Convert a list to readable text."""
        if not value:
            return ""
        items = [str(v) for v in value]
        if len(items) == 1:
            return items[0]
        if last_separator and len(items) > 1:
            return separator.join(items[:-1]) + last_separator + items[-1]
        return separator.join(items)

    @staticmethod
    def _filter_sanitize(value: str) -> str:
        """Sanitize text for safe inclusion in prompts."""
        if not value:
            return ""
        # Remove potential injection patterns
        sanitized = value.replace("{{", "{ {").replace("}}", "} }")
        # Limit length
        return sanitized[:10000]

    @staticmethod
    def _filter_first_n(value: list[Any] | str, n: int) -> list[Any] | str:
        """Get first n items from list or characters from string."""
        return value[:n]

    @staticmethod
    def _filter_keys(value: dict[str, Any]) -> list[str]:
        """Get dictionary keys as list."""
        if isinstance(value, dict):
            return list(value.keys())
        return []

    @staticmethod
    def _filter_values(value: dict[str, Any]) -> list[Any]:
        """Get dictionary values as list."""
        if isinstance(value, dict):
            return list(value.values())
        return []

    def get_required_variables(self, template: str) -> set[str]:
        """
        Get the set of required variables in a template.

        Args:
            template: The template string.

        Returns:
            Set of variable names.
        """
        from jinja2 import meta

        try:
            ast = self._env.parse(template)
            return meta.find_undeclared_variables(ast)
        except TemplateSyntaxError:
            return set()

    def validate_template(self, template: str) -> list[str]:
        """
        Validate a template for syntax errors.

        Args:
            template: The template string.

        Returns:
            List of error messages (empty if valid).
        """
        errors = []

        try:
            self._env.parse(template)
        except TemplateSyntaxError as e:
            errors.append(f"Syntax error at line {e.lineno}: {e.message}")

        return errors

    def clear_cache(self) -> None:
        """Clear the template cache."""
        self._cache.clear()
