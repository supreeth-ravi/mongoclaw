"""Main CLI entry point."""

from __future__ import annotations

import click
from rich.console import Console

from mongoclaw import __version__

console = Console()


@click.group()
@click.version_option(version=__version__, prog_name="mongoclaw")
@click.pass_context
def cli(ctx: click.Context) -> None:
    """
    MongoClaw - Declarative AI agents framework for MongoDB.

    Use 'mongoclaw COMMAND --help' for more information on a command.
    """
    ctx.ensure_object(dict)


# Import and register command groups
from mongoclaw.cli.agents import agents
from mongoclaw.cli.server import server
from mongoclaw.cli.test import test

cli.add_command(agents)
cli.add_command(server)
cli.add_command(test)


@cli.command()
def info() -> None:
    """Show application information."""
    from mongoclaw.core.config import get_settings

    settings = get_settings()

    console.print(f"[bold]MongoClaw[/bold] v{__version__}")
    console.print(f"Environment: {settings.environment}")
    console.print(f"MongoDB: {settings.mongodb.uri.get_secret_value().split('@')[-1]}")
    console.print(f"Redis: {settings.redis.url.get_secret_value().split('@')[-1]}")
    console.print(f"AI Provider: {settings.ai.default_provider}")
    console.print(f"AI Model: {settings.ai.default_model}")


@cli.command()
@click.option("--format", "output_format", type=click.Choice(["yaml", "json", "env"]), default="yaml")
def config(output_format: str) -> None:
    """Show current configuration."""
    from mongoclaw.core.config import get_settings
    import json
    import yaml

    settings = get_settings()

    # Convert to dict, excluding secrets
    config_dict = settings.model_dump(mode="json")

    # Redact secrets
    def redact(obj):
        if isinstance(obj, dict):
            return {
                k: "***" if "secret" in k.lower() or "password" in k.lower() or "key" in k.lower() or "token" in k.lower() or "uri" in k.lower() or "url" in k.lower()
                else redact(v)
                for k, v in obj.items()
            }
        elif isinstance(obj, list):
            return [redact(v) for v in obj]
        return obj

    redacted = redact(config_dict)

    if output_format == "json":
        console.print(json.dumps(redacted, indent=2))
    elif output_format == "env":
        def flatten(obj, prefix=""):
            items = []
            for k, v in obj.items():
                key = f"{prefix}_{k}".upper() if prefix else k.upper()
                if isinstance(v, dict):
                    items.extend(flatten(v, key))
                else:
                    items.append(f"MONGOCLAW_{key}={v}")
            return items
        for line in flatten(redacted):
            console.print(line)
    else:
        console.print(yaml.dump(redacted, default_flow_style=False))


@cli.command()
@click.option("--component", "-c", help="Component to check")
def health(component: str | None) -> None:
    """Check system health."""
    import asyncio

    async def check_health():
        from mongoclaw.core.config import get_settings
        from motor.motor_asyncio import AsyncIOMotorClient
        import redis.asyncio as redis

        settings = get_settings()
        results = {}

        # Check MongoDB
        if not component or component == "mongodb":
            try:
                client = AsyncIOMotorClient(
                    settings.mongodb.uri.get_secret_value(),
                    serverSelectionTimeoutMS=5000,
                )
                await client.admin.command("ping")
                results["mongodb"] = {"status": "healthy"}
                client.close()
            except Exception as e:
                results["mongodb"] = {"status": "unhealthy", "error": str(e)}

        # Check Redis
        if not component or component == "redis":
            try:
                client = redis.from_url(settings.redis.url.get_secret_value())
                await client.ping()
                results["redis"] = {"status": "healthy"}
                await client.close()
            except Exception as e:
                results["redis"] = {"status": "unhealthy", "error": str(e)}

        return results

    results = asyncio.run(check_health())

    for comp, status in results.items():
        if status["status"] == "healthy":
            console.print(f"[green]✓[/green] {comp}: healthy")
        else:
            console.print(f"[red]✗[/red] {comp}: unhealthy - {status.get('error', 'unknown')}")


if __name__ == "__main__":
    cli()
