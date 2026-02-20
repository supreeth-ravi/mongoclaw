"""Test and dry-run CLI commands."""

from __future__ import annotations

import asyncio
import json

import click
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax

console = Console()


@click.group()
def test() -> None:
    """Testing and dry-run commands."""
    pass


@test.command("agent")
@click.argument("agent_id")
@click.option("--document", "-d", help="JSON document to test with")
@click.option("--file", "-f", "doc_file", type=click.Path(exists=True), help="File containing test document")
@click.option("--dry-run", is_flag=True, default=True, help="Don't write results")
def test_agent(agent_id: str, document: str | None, doc_file: str | None, dry_run: bool) -> None:
    """Test an agent with a sample document."""
    if not document and not doc_file:
        console.print("[red]Provide either --document or --file[/red]")
        raise SystemExit(1)

    # Load document
    if doc_file:
        with open(doc_file) as f:
            test_doc = json.load(f)
    else:
        test_doc = json.loads(document)

    async def _test():
        from mongoclaw.core.config import get_settings
        from mongoclaw.agents.store import AgentStore
        from mongoclaw.ai.provider_router import ProviderRouter
        from mongoclaw.ai.prompt_engine import PromptEngine
        from mongoclaw.ai.response_parser import ResponseParser
        from motor.motor_asyncio import AsyncIOMotorClient

        settings = get_settings()
        client = AsyncIOMotorClient(settings.mongodb.uri.get_secret_value())
        store = AgentStore(
            client=client,
            database=settings.mongodb.database,
            collection=settings.mongodb.agents_collection,
        )

        agent = await store.get(agent_id)
        client.close()

        # Render prompt
        engine = PromptEngine()
        context = engine.build_context(document=test_doc)
        prompt = engine.render(agent.ai.prompt, context)

        console.print(Panel(prompt, title="Rendered Prompt"))

        # Call AI
        if not dry_run or click.confirm("Call AI provider?"):
            router = ProviderRouter(settings)
            response = await router.complete(
                model=agent.ai.model,
                prompt=prompt,
                system_prompt=agent.ai.system_prompt,
                temperature=agent.ai.temperature,
                max_tokens=agent.ai.max_tokens,
            )

            console.print(Panel(response.content, title="AI Response"))

            # Parse response
            parser = ResponseParser()
            try:
                parsed = parser.parse(response, agent.ai.response_schema)
                console.print(Panel(
                    Syntax(json.dumps(parsed, indent=2), "json"),
                    title="Parsed Response",
                ))
            except Exception as e:
                console.print(f"[yellow]Parse warning: {e}[/yellow]")

            # Show stats
            console.print(f"\n[dim]Model: {response.model}[/dim]")
            console.print(f"[dim]Tokens: {response.total_tokens} (prompt: {response.prompt_tokens}, completion: {response.completion_tokens})[/dim]")
            console.print(f"[dim]Cost: ${response.cost_usd:.6f}[/dim]")
            console.print(f"[dim]Latency: {response.latency_ms:.0f}ms[/dim]")

    asyncio.run(_test())


@test.command("prompt")
@click.argument("agent_id")
@click.option("--document", "-d", required=True, help="JSON document")
def test_prompt(agent_id: str, document: str) -> None:
    """Test prompt rendering for an agent."""
    test_doc = json.loads(document)

    async def _test():
        from mongoclaw.core.config import get_settings
        from mongoclaw.agents.store import AgentStore
        from mongoclaw.ai.prompt_engine import PromptEngine
        from motor.motor_asyncio import AsyncIOMotorClient

        settings = get_settings()
        client = AsyncIOMotorClient(settings.mongodb.uri.get_secret_value())
        store = AgentStore(
            client=client,
            database=settings.mongodb.database,
            collection=settings.mongodb.agents_collection,
        )

        agent = await store.get(agent_id)
        client.close()

        engine = PromptEngine()
        context = engine.build_context(document=test_doc)

        # Render main prompt
        prompt = engine.render(agent.ai.prompt, context)
        console.print(Panel(prompt, title="Rendered Prompt"))

        # Render system prompt if present
        if agent.ai.system_prompt:
            system = engine.render(agent.ai.system_prompt, context)
            console.print(Panel(system, title="System Prompt"))

        # Show variables
        variables = engine.get_required_variables(agent.ai.prompt)
        if variables:
            console.print(f"\n[dim]Template variables: {', '.join(sorted(variables))}[/dim]")

    asyncio.run(_test())


@test.command("connection")
def test_connection() -> None:
    """Test connections to MongoDB and Redis."""
    import asyncio

    async def _test():
        from mongoclaw.core.config import get_settings
        from motor.motor_asyncio import AsyncIOMotorClient
        import redis.asyncio as redis

        settings = get_settings()
        results = {}

        # Test MongoDB
        console.print("Testing MongoDB connection...")
        try:
            client = AsyncIOMotorClient(
                settings.mongodb.uri.get_secret_value(),
                serverSelectionTimeoutMS=5000,
            )
            await client.admin.command("ping")
            server_info = await client.server_info()
            results["mongodb"] = {
                "status": "connected",
                "version": server_info.get("version"),
            }
            client.close()
            console.print("[green]  ✓ MongoDB connected[/green]")
        except Exception as e:
            results["mongodb"] = {"status": "failed", "error": str(e)}
            console.print(f"[red]  ✗ MongoDB failed: {e}[/red]")

        # Test Redis
        console.print("Testing Redis connection...")
        try:
            client = redis.from_url(settings.redis.url.get_secret_value())
            info = await client.info("server")
            results["redis"] = {
                "status": "connected",
                "version": info.get("redis_version"),
            }
            await client.close()
            console.print("[green]  ✓ Redis connected[/green]")
        except Exception as e:
            results["redis"] = {"status": "failed", "error": str(e)}
            console.print(f"[red]  ✗ Redis failed: {e}[/red]")

        return results

    asyncio.run(_test())


@test.command("ai")
@click.option("--model", default=None, help="Model to test")
@click.option("--prompt", default="Say 'hello' in one word.", help="Test prompt")
def test_ai(model: str | None, prompt: str) -> None:
    """Test AI provider connectivity."""

    async def _test():
        from mongoclaw.core.config import get_settings
        from mongoclaw.ai.provider_router import ProviderRouter

        settings = get_settings()
        test_model = model or settings.ai.default_model

        console.print(f"Testing AI provider with model: {test_model}")

        router = ProviderRouter(settings)

        try:
            response = await router.complete(
                model=test_model,
                prompt=prompt,
                max_tokens=50,
            )

            console.print(f"[green]  ✓ AI provider connected[/green]")
            console.print(f"  Response: {response.content}")
            console.print(f"  Tokens: {response.total_tokens}")
            console.print(f"  Cost: ${response.cost_usd:.6f}")
            console.print(f"  Latency: {response.latency_ms:.0f}ms")

        except Exception as e:
            console.print(f"[red]  ✗ AI provider failed: {e}[/red]")
            raise SystemExit(1)

    asyncio.run(_test())
