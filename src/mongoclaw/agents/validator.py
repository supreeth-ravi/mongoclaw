"""Validation for agent configurations."""

from __future__ import annotations

import re
from typing import Any

from jinja2 import Environment, TemplateSyntaxError, UndefinedError, meta

from mongoclaw.agents.models import AgentConfig
from mongoclaw.core.exceptions import ValidationError


class AgentValidator:
    """Validator for agent configurations."""

    # Supported AI providers and their model patterns
    PROVIDER_MODEL_PATTERNS: dict[str, list[str]] = {
        "openai": [r"gpt-4.*", r"gpt-3\.5.*", r"o1.*", r"o3.*"],
        "anthropic": [r"claude-.*"],
        "azure": [r".*"],  # Azure uses deployment names
        "bedrock": [r"anthropic\.claude.*", r"amazon\.titan.*", r"ai21\..*", r"cohere\..*"],
        "google": [r"gemini-.*", r"palm-.*"],
        "cohere": [r"command.*"],
        "groq": [r"llama.*", r"mixtral.*", r"gemma.*"],
        "mistral": [r"mistral-.*", r"mixtral-.*"],
        "ollama": [r".*"],  # Ollama supports any model name
        "together": [r".*"],  # Together supports various models
    }

    # Reserved field names that should not be used in write configs
    RESERVED_FIELDS = {"_id", "_mongoclaw_version"}

    def __init__(self, jinja_env: Environment | None = None) -> None:
        self._jinja_env = jinja_env or Environment()

    def validate(self, config: AgentConfig) -> list[str]:
        """
        Validate an agent configuration.

        Args:
            config: The configuration to validate.

        Returns:
            List of validation errors (empty if valid).
        """
        errors: list[str] = []

        errors.extend(self._validate_watch(config))
        errors.extend(self._validate_ai(config))
        errors.extend(self._validate_write(config))
        errors.extend(self._validate_execution(config))

        return errors

    def validate_or_raise(self, config: AgentConfig) -> None:
        """
        Validate and raise if errors found.

        Args:
            config: The configuration to validate.

        Raises:
            ValidationError: If validation fails.
        """
        errors = self.validate(config)
        if errors:
            raise ValidationError(
                f"Agent configuration validation failed with {len(errors)} error(s)",
                details={"errors": errors, "agent_id": config.id},
            )

    def _validate_watch(self, config: AgentConfig) -> list[str]:
        """Validate watch configuration."""
        errors: list[str] = []
        watch = config.watch

        # Validate database name
        if not self._is_valid_mongodb_name(watch.database):
            errors.append(
                f"Invalid database name: '{watch.database}'. "
                "Must not contain '/', '\\', '.', ' ', '\"', '$', or null characters."
            )

        # Validate collection name
        if not self._is_valid_mongodb_name(watch.collection):
            errors.append(
                f"Invalid collection name: '{watch.collection}'. "
                "Must not contain '$' or null characters, and cannot start with 'system.'."
            )

        if watch.collection.startswith("system."):
            errors.append(f"Cannot watch system collection: '{watch.collection}'")

        # Validate filter if present
        if watch.filter:
            filter_errors = self._validate_mongodb_filter(watch.filter)
            errors.extend(filter_errors)

        return errors

    def _validate_ai(self, config: AgentConfig) -> list[str]:
        """Validate AI configuration."""
        errors: list[str] = []
        ai = config.ai

        # Validate provider
        provider = ai.provider.lower()
        if provider not in self.PROVIDER_MODEL_PATTERNS and provider != "custom":
            errors.append(
                f"Unknown AI provider: '{ai.provider}'. "
                f"Supported providers: {', '.join(self.PROVIDER_MODEL_PATTERNS.keys())}, custom"
            )

        # Validate model for known providers
        if provider in self.PROVIDER_MODEL_PATTERNS:
            patterns = self.PROVIDER_MODEL_PATTERNS[provider]
            if not any(re.match(p, ai.model) for p in patterns):
                errors.append(
                    f"Model '{ai.model}' may not be valid for provider '{ai.provider}'. "
                    f"Expected patterns: {patterns}"
                )

        # Validate prompt template
        prompt_errors = self._validate_jinja_template(ai.prompt, "prompt")
        errors.extend(prompt_errors)

        # Validate system prompt if present
        if ai.system_prompt:
            system_errors = self._validate_jinja_template(ai.system_prompt, "system_prompt")
            errors.extend(system_errors)

        # Validate response schema if present
        if ai.response_schema:
            schema_errors = self._validate_json_schema(ai.response_schema)
            errors.extend(schema_errors)

        return errors

    def _validate_write(self, config: AgentConfig) -> list[str]:
        """Validate write configuration."""
        errors: list[str] = []
        write = config.write

        # Check for reserved fields
        if write.fields:
            for field in write.fields.values():
                if field in self.RESERVED_FIELDS:
                    errors.append(f"Cannot write to reserved field: '{field}'")

        # Validate idempotency key template if present
        if write.idempotency_key:
            key_errors = self._validate_jinja_template(
                write.idempotency_key, "idempotency_key"
            )
            errors.extend(key_errors)

        # Validate nested path format
        if write.path:
            if not self._is_valid_field_path(write.path):
                errors.append(f"Invalid nested path: '{write.path}'")

        return errors

    def _validate_execution(self, config: AgentConfig) -> list[str]:
        """Validate execution configuration."""
        errors: list[str] = []
        execution = config.execution

        # Validate retry delay ordering
        if execution.retry_delay_seconds > execution.retry_max_delay_seconds:
            errors.append(
                f"retry_delay_seconds ({execution.retry_delay_seconds}) "
                f"cannot exceed retry_max_delay_seconds ({execution.retry_max_delay_seconds})"
            )

        # Validate timeout vs retry delay
        total_retry_time = (
            execution.retry_delay_seconds * (2 ** execution.max_retries - 1)
        )
        if total_retry_time > execution.timeout_seconds * 2:
            errors.append(
                f"Total potential retry time ({total_retry_time}s) "
                f"significantly exceeds timeout ({execution.timeout_seconds}s). "
                "Consider reducing max_retries or increasing timeout."
            )

        return errors

    def _validate_jinja_template(
        self, template: str, field_name: str
    ) -> list[str]:
        """Validate a Jinja2 template."""
        errors: list[str] = []

        try:
            ast = self._jinja_env.parse(template)
            # Check for undefined variables (informational, not error)
            variables = meta.find_undeclared_variables(ast)
            # Basic variable validation
            for var in variables:
                if not var.isidentifier():
                    errors.append(
                        f"Invalid variable name in {field_name}: '{var}'"
                    )
        except TemplateSyntaxError as e:
            errors.append(f"Template syntax error in {field_name}: {e.message}")

        return errors

    def _validate_json_schema(self, schema: dict[str, Any]) -> list[str]:
        """Validate a JSON schema structure."""
        errors: list[str] = []

        if "type" not in schema:
            errors.append("JSON schema must have a 'type' field")
            return errors

        valid_types = {"object", "array", "string", "number", "integer", "boolean", "null"}
        schema_type = schema.get("type")

        if schema_type not in valid_types:
            errors.append(f"Invalid JSON schema type: '{schema_type}'")

        if schema_type == "object" and "properties" not in schema:
            errors.append("Object schema should have 'properties' field")

        if schema_type == "array" and "items" not in schema:
            errors.append("Array schema should have 'items' field")

        return errors

    def _validate_mongodb_filter(self, filter_doc: dict[str, Any]) -> list[str]:
        """Validate a MongoDB filter document."""
        errors: list[str] = []

        def check_operators(doc: dict[str, Any], path: str = "") -> None:
            for key, value in doc.items():
                current_path = f"{path}.{key}" if path else key

                # Check for potentially dangerous operators
                if key.startswith("$"):
                    dangerous_ops = {"$where", "$function", "$accumulator"}
                    if key in dangerous_ops:
                        errors.append(
                            f"Dangerous operator '{key}' not allowed in filter at '{current_path}'"
                        )

                # Recursively check nested documents
                if isinstance(value, dict):
                    check_operators(value, current_path)
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            check_operators(item, f"{current_path}[{i}]")

        check_operators(filter_doc)
        return errors

    def _is_valid_mongodb_name(self, name: str) -> bool:
        """Check if a name is valid for MongoDB database/collection."""
        if not name or len(name) > 120:
            return False

        invalid_chars = set('/\\. "$\x00')
        return not any(c in invalid_chars for c in name)

    def _is_valid_field_path(self, path: str) -> bool:
        """Check if a field path is valid."""
        if not path:
            return False

        parts = path.split(".")
        for part in parts:
            # Must be a valid identifier or array index
            if not part:
                return False
            if part.startswith("$"):
                return False
            # Allow array notation like "field.0.subfield"
            if not (part.isidentifier() or part.isdigit()):
                return False

        return True

    def get_template_variables(self, template: str) -> set[str]:
        """
        Get all variables used in a Jinja2 template.

        Args:
            template: The template string.

        Returns:
            Set of variable names.
        """
        try:
            ast = self._jinja_env.parse(template)
            return meta.find_undeclared_variables(ast)
        except TemplateSyntaxError:
            return set()

    def validate_prompt_variables(
        self,
        config: AgentConfig,
        available_variables: set[str],
    ) -> list[str]:
        """
        Validate that prompt templates only use available variables.

        Args:
            config: The agent configuration.
            available_variables: Set of variables that will be available at runtime.

        Returns:
            List of validation errors.
        """
        errors: list[str] = []

        # Check main prompt
        prompt_vars = self.get_template_variables(config.ai.prompt)
        missing = prompt_vars - available_variables
        if missing:
            errors.append(
                f"Prompt uses undefined variables: {', '.join(sorted(missing))}. "
                f"Available: {', '.join(sorted(available_variables))}"
            )

        # Check system prompt
        if config.ai.system_prompt:
            system_vars = self.get_template_variables(config.ai.system_prompt)
            missing = system_vars - available_variables
            if missing:
                errors.append(
                    f"System prompt uses undefined variables: {', '.join(sorted(missing))}"
                )

        return errors
