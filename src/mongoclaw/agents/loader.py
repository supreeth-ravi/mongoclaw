"""YAML/JSON loader for agent configurations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from mongoclaw.agents.models import AgentConfig
from mongoclaw.agents.validator import AgentValidator
from mongoclaw.core.exceptions import ConfigurationError, ValidationError


class AgentLoader:
    """Loader for agent configurations from files."""

    def __init__(self, validator: AgentValidator | None = None) -> None:
        self._validator = validator or AgentValidator()

    def load_file(self, path: str | Path, validate: bool = True) -> AgentConfig:
        """
        Load an agent configuration from a file.

        Args:
            path: Path to YAML or JSON file.
            validate: Whether to validate the configuration.

        Returns:
            The loaded agent configuration.

        Raises:
            ConfigurationError: If file cannot be loaded.
            ValidationError: If validation fails.
        """
        file_path = Path(path)

        if not file_path.exists():
            raise ConfigurationError(f"Configuration file not found: {path}")

        try:
            content = file_path.read_text(encoding="utf-8")
        except OSError as e:
            raise ConfigurationError(f"Failed to read configuration file: {e}")

        return self.load_string(content, file_path.suffix, validate=validate)

    def load_string(
        self,
        content: str,
        format_hint: str = ".yaml",
        validate: bool = True,
    ) -> AgentConfig:
        """
        Load an agent configuration from a string.

        Args:
            content: The configuration content.
            format_hint: File extension hint (".yaml", ".json").
            validate: Whether to validate the configuration.

        Returns:
            The loaded agent configuration.

        Raises:
            ConfigurationError: If content cannot be parsed.
            ValidationError: If validation fails.
        """
        try:
            if format_hint.lower() in (".json",):
                data = json.loads(content)
            else:
                data = yaml.safe_load(content)
        except (json.JSONDecodeError, yaml.YAMLError) as e:
            raise ConfigurationError(f"Failed to parse configuration: {e}")

        if not isinstance(data, dict):
            raise ConfigurationError("Configuration must be a mapping/object")

        return self.load_dict(data, validate=validate)

    def load_dict(self, data: dict[str, Any], validate: bool = True) -> AgentConfig:
        """
        Load an agent configuration from a dictionary.

        Args:
            data: The configuration dictionary.
            validate: Whether to validate the configuration.

        Returns:
            The loaded agent configuration.

        Raises:
            ValidationError: If validation fails.
        """
        try:
            config = AgentConfig.model_validate(data)
        except Exception as e:
            raise ValidationError(f"Invalid agent configuration: {e}")

        if validate:
            self._validator.validate_or_raise(config)

        return config

    def load_directory(
        self,
        path: str | Path,
        validate: bool = True,
        recursive: bool = False,
    ) -> list[AgentConfig]:
        """
        Load all agent configurations from a directory.

        Args:
            path: Directory path.
            validate: Whether to validate configurations.
            recursive: Whether to search recursively.

        Returns:
            List of loaded agent configurations.

        Raises:
            ConfigurationError: If directory doesn't exist.
        """
        dir_path = Path(path)

        if not dir_path.is_dir():
            raise ConfigurationError(f"Not a directory: {path}")

        configs: list[AgentConfig] = []
        errors: list[tuple[str, str]] = []

        pattern = "**/*" if recursive else "*"
        for file_path in dir_path.glob(pattern):
            if file_path.suffix.lower() not in (".yaml", ".yml", ".json"):
                continue

            if not file_path.is_file():
                continue

            try:
                config = self.load_file(file_path, validate=validate)
                configs.append(config)
            except (ConfigurationError, ValidationError) as e:
                errors.append((str(file_path), str(e)))

        if errors:
            error_details = "; ".join(f"{p}: {e}" for p, e in errors)
            raise ConfigurationError(
                f"Failed to load {len(errors)} configuration(s): {error_details}",
                details={"errors": dict(errors)},
            )

        return configs

    def dump_yaml(self, config: AgentConfig) -> str:
        """
        Dump an agent configuration to YAML.

        Args:
            config: The agent configuration.

        Returns:
            YAML string.
        """
        data = config.model_dump(mode="json", exclude_none=True)
        return yaml.dump(data, default_flow_style=False, sort_keys=False)

    def dump_json(self, config: AgentConfig, pretty: bool = True) -> str:
        """
        Dump an agent configuration to JSON.

        Args:
            config: The agent configuration.
            pretty: Whether to format with indentation.

        Returns:
            JSON string.
        """
        data = config.model_dump(mode="json", exclude_none=True)
        if pretty:
            return json.dumps(data, indent=2)
        return json.dumps(data)

    def save_file(
        self,
        config: AgentConfig,
        path: str | Path,
        overwrite: bool = False,
    ) -> None:
        """
        Save an agent configuration to a file.

        Args:
            config: The agent configuration.
            path: Output file path.
            overwrite: Whether to overwrite existing file.

        Raises:
            ConfigurationError: If file exists and overwrite is False.
        """
        file_path = Path(path)

        if file_path.exists() and not overwrite:
            raise ConfigurationError(f"File already exists: {path}")

        # Determine format from extension
        if file_path.suffix.lower() in (".json",):
            content = self.dump_json(config)
        else:
            content = self.dump_yaml(config)

        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding="utf-8")


def create_example_config() -> AgentConfig:
    """Create an example agent configuration."""
    return AgentConfig(
        id="ticket-classifier",
        name="Ticket Classifier",
        description="Classifies support tickets using AI",
        watch={
            "database": "support",
            "collection": "tickets",
            "operations": ["insert", "update"],
            "filter": {"status": "new"},
        },
        ai={
            "provider": "openai",
            "model": "gpt-4o-mini",
            "prompt": """Classify this support ticket:

Title: {{ document.title }}
Description: {{ document.description }}

Respond with JSON containing:
- category: one of [billing, technical, general, urgent]
- priority: one of [low, medium, high, critical]
- summary: brief summary in 1-2 sentences
""",
            "temperature": 0.3,
            "response_schema": {
                "type": "object",
                "properties": {
                    "category": {"type": "string"},
                    "priority": {"type": "string"},
                    "summary": {"type": "string"},
                },
                "required": ["category", "priority", "summary"],
            },
        },
        write={
            "strategy": "merge",
            "fields": {
                "category": "ai_category",
                "priority": "ai_priority",
                "summary": "ai_summary",
            },
        },
        execution={
            "max_retries": 3,
            "timeout_seconds": 30,
        },
        tags=["support", "classifier"],
    )
