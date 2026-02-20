"""FastAPI application factory."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from mongoclaw import __version__
from mongoclaw.core.config import Settings, get_settings
from mongoclaw.observability.logging import configure_logging, get_logger
from mongoclaw.observability.middleware import setup_middleware
from mongoclaw.observability.metrics import get_metrics_collector

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan handler."""
    settings = get_settings()

    # Startup
    logger.info(
        "Starting MongoClaw API",
        version=__version__,
        environment=settings.environment,
    )

    # Initialize metrics
    collector = get_metrics_collector()
    collector.initialize(version=__version__)

    yield

    # Shutdown
    logger.info("Shutting down MongoClaw API")


def create_app(settings: Settings | None = None) -> FastAPI:
    """
    Create and configure the FastAPI application.

    Args:
        settings: Optional settings override.

    Returns:
        Configured FastAPI application.
    """
    settings = settings or get_settings()

    # Configure logging
    configure_logging(
        level=settings.observability.log_level,
        format_type=settings.observability.log_format,
    )

    # Create FastAPI app
    app = FastAPI(
        title="MongoClaw API",
        description="Declarative AI agents framework for MongoDB",
        version=__version__,
        docs_url="/docs" if settings.debug else None,
        redoc_url="/redoc" if settings.debug else None,
        lifespan=lifespan,
    )

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.api.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Observability middleware
    setup_middleware(app)

    # Include routers
    from mongoclaw.api.v1 import agents, executions, health, webhooks

    app.include_router(
        health.router,
        tags=["Health"],
    )

    app.include_router(
        agents.router,
        prefix="/api/v1/agents",
        tags=["Agents"],
    )

    app.include_router(
        executions.router,
        prefix="/api/v1/executions",
        tags=["Executions"],
    )

    app.include_router(
        webhooks.router,
        prefix="/api/v1/webhooks",
        tags=["Webhooks"],
    )

    # Store settings in app state
    app.state.settings = settings

    logger.info("FastAPI application created")

    return app


def get_app() -> FastAPI:
    """Get the default application instance."""
    return create_app()
