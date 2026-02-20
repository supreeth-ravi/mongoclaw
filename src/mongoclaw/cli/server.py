"""Server management CLI commands."""

from __future__ import annotations

import click
from rich.console import Console

console = Console()


@click.group()
def server() -> None:
    """Server management commands."""
    pass


@server.command("start")
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", default=8000, type=int, help="Port to bind to")
@click.option("--workers", default=1, type=int, help="Number of worker processes")
@click.option("--reload", is_flag=True, help="Enable auto-reload")
@click.option("--api-only", is_flag=True, help="Start only the API server")
def start_server(
    host: str,
    port: int,
    workers: int,
    reload: bool,
    api_only: bool,
) -> None:
    """Start the MongoClaw server."""
    import uvicorn

    console.print(f"[bold]Starting MongoClaw server[/bold]")
    console.print(f"  Host: {host}")
    console.print(f"  Port: {port}")
    console.print(f"  Workers: {workers}")
    console.print(f"  Reload: {reload}")
    console.print(f"  API Only: {api_only}")

    if api_only:
        # Start only the FastAPI server
        uvicorn.run(
            "mongoclaw.api.app:get_app",
            host=host,
            port=port,
            workers=workers,
            reload=reload,
            factory=True,
        )
    else:
        # Start full runtime with API
        import asyncio
        from mongoclaw.core.runtime import get_runtime

        async def run_full():
            runtime = get_runtime()
            await runtime.start()

            # Also start API server in background
            config = uvicorn.Config(
                "mongoclaw.api.app:get_app",
                host=host,
                port=port,
                factory=True,
            )
            server = uvicorn.Server(config)

            await asyncio.gather(
                runtime.run_forever(),
                server.serve(),
            )

        asyncio.run(run_full())


@server.command("run")
@click.option("--no-api", is_flag=True, help="Don't start the API server")
def run_runtime(no_api: bool) -> None:
    """Run the MongoClaw runtime (without API)."""
    import asyncio
    from mongoclaw.core.runtime import get_runtime

    console.print("[bold]Starting MongoClaw runtime[/bold]")

    async def run():
        runtime = get_runtime()
        await runtime.run_forever()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        console.print("\n[yellow]Shutting down...[/yellow]")


@server.command("api")
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", default=8000, type=int, help="Port to bind to")
@click.option("--reload", is_flag=True, help="Enable auto-reload")
def start_api(host: str, port: int, reload: bool) -> None:
    """Start only the API server."""
    import uvicorn

    console.print(f"[bold]Starting API server[/bold] on {host}:{port}")

    uvicorn.run(
        "mongoclaw.api.app:get_app",
        host=host,
        port=port,
        reload=reload,
        factory=True,
    )


@server.command("status")
@click.option("--url", default="http://localhost:8000", help="API base URL")
def check_status(url: str) -> None:
    """Check server status."""
    import httpx

    try:
        with httpx.Client() as client:
            response = client.get(f"{url}/health")
            if response.status_code == 200:
                console.print(f"[green]✓[/green] Server is running at {url}")

                # Get detailed health
                detailed = client.get(f"{url}/health/detailed")
                if detailed.status_code == 200:
                    data = detailed.json()
                    console.print(f"  Version: {data.get('version', 'unknown')}")
                    console.print(f"  Environment: {data.get('environment', 'unknown')}")

                    components = data.get("components", {})
                    for comp, status in components.items():
                        status_str = status.get("status", "unknown")
                        if status_str == "healthy":
                            console.print(f"  {comp}: [green]healthy[/green]")
                        else:
                            console.print(f"  {comp}: [red]{status_str}[/red]")
            else:
                console.print(f"[red]✗[/red] Server returned {response.status_code}")

    except httpx.ConnectError:
        console.print(f"[red]✗[/red] Cannot connect to {url}")
        raise SystemExit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] Error: {e}")
        raise SystemExit(1)
