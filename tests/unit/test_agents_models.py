"""Unit tests for agent models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from mongoclaw.agents.models import (
    AgentConfig,
    AIConfig,
    ExecutionConfig,
    WatchConfig,
    WriteConfig,
)
from mongoclaw.core.types import ChangeOperation, WriteStrategy


class TestWatchConfig:
    """Tests for WatchConfig model."""

    def test_valid_watch_config(self):
        """Test creating a valid watch config."""
        config = WatchConfig(
            database="mydb",
            collection="mycoll",
            operations=["insert", "update"],
        )
        assert config.database == "mydb"
        assert config.collection == "mycoll"
        assert len(config.operations) == 2

    def test_default_operations(self):
        """Test default operations."""
        config = WatchConfig(database="mydb", collection="mycoll")
        assert ChangeOperation.INSERT in config.operations
        assert ChangeOperation.UPDATE in config.operations

    def test_with_filter(self):
        """Test watch config with filter."""
        config = WatchConfig(
            database="mydb",
            collection="mycoll",
            filter={"status": "active"},
        )
        assert config.filter == {"status": "active"}


class TestAIConfig:
    """Tests for AIConfig model."""

    def test_valid_ai_config(self):
        """Test creating a valid AI config."""
        config = AIConfig(
            model="gpt-4o-mini",
            prompt="Process this: {{ document.text }}",
        )
        assert config.model == "gpt-4o-mini"
        assert config.prompt == "Process this: {{ document.text }}"

    def test_default_values(self):
        """Test default AI config values."""
        config = AIConfig(model="gpt-4o-mini", prompt="test")
        assert config.temperature == 0.7
        assert config.max_tokens == 2048  # Actual default

    def test_with_response_schema(self):
        """Test AI config with response schema."""
        schema = {
            "type": "object",
            "properties": {"category": {"type": "string"}},
        }
        config = AIConfig(
            model="gpt-4o-mini",
            prompt="test",
            response_schema=schema,
        )
        assert config.response_schema == schema


class TestWriteConfig:
    """Tests for WriteConfig model."""

    def test_valid_write_config(self):
        """Test creating a valid write config."""
        config = WriteConfig(
            strategy="merge",
        )
        assert config.strategy == WriteStrategy.MERGE

    def test_merge_strategy(self):
        """Test merge write strategy."""
        config = WriteConfig(strategy="merge")
        assert config.strategy == WriteStrategy.MERGE

    def test_replace_strategy(self):
        """Test replace write strategy."""
        config = WriteConfig(strategy="replace")
        assert config.strategy == WriteStrategy.REPLACE

    def test_append_requires_array_field(self):
        """Test append strategy requires array_field."""
        with pytest.raises(ValueError):
            WriteConfig(strategy="append")

        # With array_field it works
        config = WriteConfig(strategy="append", array_field="items")
        assert config.strategy == WriteStrategy.APPEND


class TestExecutionConfig:
    """Tests for ExecutionConfig model."""

    def test_default_values(self):
        """Test default execution config values."""
        config = ExecutionConfig()
        assert config.max_retries == 3
        assert config.retry_delay_seconds == 1.0
        assert config.timeout_seconds == 60.0

    def test_custom_values(self):
        """Test custom execution config values."""
        config = ExecutionConfig(
            max_retries=5,
            retry_delay_seconds=2.0,
            timeout_seconds=120.0,
            rate_limit_requests=100,
            cost_limit_usd=1.0,
        )
        assert config.max_retries == 5
        assert config.retry_delay_seconds == 2.0
        assert config.cost_limit_usd == 1.0


class TestAgentConfig:
    """Tests for AgentConfig model."""

    def test_valid_agent_config(self, sample_agent_config):
        """Test creating a valid agent config."""
        agent = AgentConfig(**sample_agent_config)
        assert agent.id == "test_agent"
        assert agent.name == "Test Agent"
        assert agent.enabled is True

    def test_missing_required_fields(self):
        """Test that missing required fields raise validation error."""
        with pytest.raises(ValidationError):
            AgentConfig(id="test")  # Missing required fields

    def test_agent_serialization(self, sample_agent_config):
        """Test agent serialization to dict."""
        agent = AgentConfig(**sample_agent_config)
        data = agent.model_dump()
        assert data["id"] == "test_agent"
        assert "watch" in data
        assert "ai" in data
