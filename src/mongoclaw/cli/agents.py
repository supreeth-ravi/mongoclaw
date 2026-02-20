"""Agent management CLI commands."""

from __future__ import annotations

import asyncio
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.group()
def agents() -> None:
    """Manage agent configurations."""
    pass


@agents.command("list")
@click.option("--enabled-only", is_flag=True, help="Show only enabled agents")
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), default="table")
def list_agents(enabled_only: bool, output_format: str) -> None:
    """List all agents."""

    async def _list():
        from mongoclaw.core.config import get_settings
        from mongoclaw.agents.store import AgentStore
        from motor.motor_asyncio import AsyncIOMotorClient

        settings = get_settings()
        client = AsyncIOMotorClient(settings.mongodb.uri.get_secret_value())
        store = AgentStore(
            client=client,
            database=settings.mongodb.database,
            collection=settings.mongodb.agents_collection,
        )

        agents = await store.list(enabled_only=enabled_only)
        client.close()
        return agents

    agents_list = asyncio.run(_list())

    if output_format == "json":
        import json
        data = [a.model_dump(mode="json") for a in agents_list]
        console.print(json.dumps(data, indent=2))
    else:
        table = Table(title="Agents")
        table.add_column("ID", style="cyan")
        table.add_column("Name")
        table.add_column("Collection")
        table.add_column("Model")
        table.add_column("Enabled")

        for agent in agents_list:
            table.add_row(
                agent.id,
                agent.name,
                f"{agent.watch.database}.{agent.watch.collection}",
                agent.ai.model,
                "✓" if agent.enabled else "✗",
            )

        console.print(table)


@agents.command("get")
@click.argument("agent_id")
@click.option("--format", "output_format", type=click.Choice(["yaml", "json"]), default="yaml")
def get_agent(agent_id: str, output_format: str) -> None:
    """Get an agent configuration."""

    async def _get():
        from mongoclaw.core.config import get_settings
        from mongoclaw.agents.store import AgentStore
        from motor.motor_asyncio import AsyncIOMotorClient

        settings = get_settings()
        client = AsyncIOMotorClient(settings.mongodb.uri.get_secret_value())
        store = AgentStore(
            client=client,
            database=settings.mongodb.database,
            collection=settings.mongodb.agents_collection,
        )

        agent = await store.get_optional(agent_id)
        client.close()
        return agent

    agent = asyncio.run(_get())

    if agent is None:
        console.print(f"[red]Agent '{agent_id}' not found[/red]")
        raise SystemExit(1)

    if output_format == "json":
        import json
        console.print(json.dumps(agent.model_dump(mode="json"), indent=2))
    else:
        import yaml
        console.print(yaml.dump(agent.model_dump(mode="json"), default_flow_style=False))


@agents.command("create")
@click.option("-f", "--file", "config_file", type=click.Path(exists=True), required=True)
def create_agent(config_file: str) -> None:
    """Create an agent from a config file."""

    async def _create():
        from mongoclaw.core.config import get_settings
        from mongoclaw.agents.store import AgentStore
        from mongoclaw.agents.loader import AgentLoader
        from motor.motor_asyncio import AsyncIOMotorClient

        loader = AgentLoader()
        config = loader.load_file(config_file)

        settings = get_settings()
        client = AsyncIOMotorClient(settings.mongodb.uri.get_secret_value())
        store = AgentStore(
            client=client,
            database=settings.mongodb.database,
            collection=settings.mongodb.agents_collection,
        )
        await store.initialize()

        created = await store.create(config)
        client.close()
        return created

    try:
        agent = asyncio.run(_create())
        console.print(f"[green]✓[/green] Created agent: {agent.id}")
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to create agent: {e}")
        raise SystemExit(1)


@agents.command("delete")
@click.argument("agent_id")
@click.option("--force", "-f", is_flag=True, help="Skip confirmation")
def delete_agent(agent_id: str, force: bool) -> None:
    """Delete an agent."""
    if not force:
        if not click.confirm(f"Delete agent '{agent_id}'?"):
            return

    async def _delete():
        from mongoclaw.core.config import get_settings
        from mongoclaw.agents.store import AgentStore
        from motor.motor_asyncio import AsyncIOMotorClient

        settings = get_settings()
        client = AsyncIOMotorClient(settings.mongodb.uri.get_secret_value())
        store = AgentStore(
            client=client,
            database=settings.mongodb.database,
            collection=settings.mongodb.agents_collection,
        )

        deleted = await store.delete(agent_id)
        client.close()
        return deleted

    deleted = asyncio.run(_delete())

    if deleted:
        console.print(f"[green]✓[/green] Deleted agent: {agent_id}")
    else:
        console.print(f"[yellow]Agent '{agent_id}' not found[/yellow]")


@agents.command("enable")
@click.argument("agent_id")
def enable_agent(agent_id: str) -> None:
    """Enable an agent."""

    async def _enable():
        from mongoclaw.core.config import get_settings
        from mongoclaw.agents.store import AgentStore
        from motor.motor_asyncio import AsyncIOMotorClient

        settings = get_settings()
        client = AsyncIOMotorClient(settings.mongodb.uri.get_secret_value())
        store = AgentStore(
            client=client,
            database=settings.mongodb.database,
            collection=settings.mongodb.agents_collection,
        )

        updated = await store.enable(agent_id)
        client.close()
        return updated

    updated = asyncio.run(_enable())

    if updated:
        console.print(f"[green]✓[/green] Enabled agent: {agent_id}")
    else:
        console.print(f"[yellow]Agent '{agent_id}' not found[/yellow]")


@agents.command("disable")
@click.argument("agent_id")
def disable_agent(agent_id: str) -> None:
    """Disable an agent."""

    async def _disable():
        from mongoclaw.core.config import get_settings
        from mongoclaw.agents.store import AgentStore
        from motor.motor_asyncio import AsyncIOMotorClient

        settings = get_settings()
        client = AsyncIOMotorClient(settings.mongodb.uri.get_secret_value())
        store = AgentStore(
            client=client,
            database=settings.mongodb.database,
            collection=settings.mongodb.agents_collection,
        )

        updated = await store.disable(agent_id)
        client.close()
        return updated

    updated = asyncio.run(_disable())

    if updated:
        console.print(f"[green]✓[/green] Disabled agent: {agent_id}")
    else:
        console.print(f"[yellow]Agent '{agent_id}' not found[/yellow]")


@agents.command("validate")
@click.option("-f", "--file", "config_file", type=click.Path(exists=True))
@click.option("--id", "agent_id", help="Validate existing agent by ID")
def validate_agent(config_file: str | None, agent_id: str | None) -> None:
    """Validate an agent configuration."""
    from mongoclaw.agents.validator import AgentValidator

    if not config_file and not agent_id:
        console.print("[red]Provide either --file or --id[/red]")
        raise SystemExit(1)

    async def _validate():
        if config_file:
            from mongoclaw.agents.loader import AgentLoader
            loader = AgentLoader()
            config = loader.load_file(config_file, validate=False)
        else:
            from mongoclaw.core.config import get_settings
            from mongoclaw.agents.store import AgentStore
            from motor.motor_asyncio import AsyncIOMotorClient

            settings = get_settings()
            client = AsyncIOMotorClient(settings.mongodb.uri.get_secret_value())
            store = AgentStore(
                client=client,
                database=settings.mongodb.database,
                collection=settings.mongodb.agents_collection,
            )
            config = await store.get(agent_id)
            client.close()

        validator = AgentValidator()
        errors = validator.validate(config)
        return config, errors

    config, errors = asyncio.run(_validate())

    if errors:
        console.print(f"[red]✗[/red] Validation failed for '{config.id}':")
        for error in errors:
            console.print(f"  - {error}")
        raise SystemExit(1)
    else:
        console.print(f"[green]✓[/green] Agent '{config.id}' is valid")
