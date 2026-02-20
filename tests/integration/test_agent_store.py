"""Integration tests for agent store."""

from __future__ import annotations

import pytest
import pytest_asyncio

from mongoclaw.agents.models import AgentConfig
from mongoclaw.agents.store import AgentStore


@pytest.mark.asyncio
class TestAgentStore:
    """Integration tests for AgentStore."""

    async def test_create_agent(self, agent_store, sample_agent_config):
        """Test creating an agent."""
        config = AgentConfig(**sample_agent_config)
        created = await agent_store.create(config)

        assert created.id == config.id
        assert created.name == config.name

    async def test_get_agent(self, agent_store, sample_agent_config):
        """Test getting an agent by ID."""
        config = AgentConfig(**sample_agent_config)
        await agent_store.create(config)

        retrieved = await agent_store.get(config.id)
        assert retrieved.id == config.id
        assert retrieved.name == config.name

    async def test_get_nonexistent_agent(self, agent_store):
        """Test getting a nonexistent agent raises error."""
        with pytest.raises(Exception):
            await agent_store.get("nonexistent_id")

    async def test_get_optional_returns_none(self, agent_store):
        """Test get_optional returns None for nonexistent agent."""
        result = await agent_store.get_optional("nonexistent_id")
        assert result is None

    async def test_list_agents(self, agent_store, sample_agent_config):
        """Test listing agents."""
        config = AgentConfig(**sample_agent_config)
        await agent_store.create(config)

        agents = await agent_store.list()
        assert len(agents) >= 1
        assert any(a.id == config.id for a in agents)

    async def test_list_enabled_only(self, agent_store, sample_agent_config):
        """Test listing only enabled agents."""
        # Create enabled agent
        config = AgentConfig(**sample_agent_config)
        await agent_store.create(config)

        # Create disabled agent
        disabled_config = sample_agent_config.copy()
        disabled_config["id"] = "disabled_agent"
        disabled_config["enabled"] = False
        await agent_store.create(AgentConfig(**disabled_config))

        enabled_agents = await agent_store.list(enabled_only=True)
        assert all(a.enabled for a in enabled_agents)

    async def test_update_agent(self, agent_store, sample_agent_config):
        """Test updating an agent."""
        config = AgentConfig(**sample_agent_config)
        await agent_store.create(config)

        updated_config = sample_agent_config.copy()
        updated_config["name"] = "Updated Name"
        updated = await agent_store.update(config.id, AgentConfig(**updated_config))

        assert updated.name == "Updated Name"

    async def test_delete_agent(self, agent_store, sample_agent_config):
        """Test deleting an agent."""
        config = AgentConfig(**sample_agent_config)
        await agent_store.create(config)

        deleted = await agent_store.delete(config.id)
        assert deleted is True

        result = await agent_store.get_optional(config.id)
        assert result is None

    async def test_enable_disable_agent(self, agent_store, sample_agent_config):
        """Test enabling and disabling an agent."""
        config = AgentConfig(**sample_agent_config)
        await agent_store.create(config)

        # Disable
        await agent_store.disable(config.id)
        agent = await agent_store.get(config.id)
        assert agent.enabled is False

        # Enable
        await agent_store.enable(config.id)
        agent = await agent_store.get(config.id)
        assert agent.enabled is True
