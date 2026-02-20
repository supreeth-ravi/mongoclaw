"""Structured logging configuration using structlog."""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.types import Processor

from mongoclaw.core.config import LogLevel, get_settings


def configure_logging(
    level: LogLevel | str | None = None,
    format_type: str | None = None,
    service_name: str | None = None,
) -> None:
    """
    Configure structured logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        format_type: Output format ("json" or "console").
        service_name: Service name for log entries.
    """
    settings = get_settings()

    log_level = level or settings.observability.log_level
    if isinstance(log_level, str):
        log_level = LogLevel(log_level.upper())

    output_format = format_type or settings.observability.log_format
    svc_name = service_name or settings.observability.service_name

    # Convert LogLevel enum to logging level
    level_map = {
        LogLevel.DEBUG: logging.DEBUG,
        LogLevel.INFO: logging.INFO,
        LogLevel.WARNING: logging.WARNING,
        LogLevel.ERROR: logging.ERROR,
        LogLevel.CRITICAL: logging.CRITICAL,
    }
    numeric_level = level_map.get(log_level, logging.INFO)

    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=numeric_level,
    )

    # Build processor chain
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if output_format == "json":
        # JSON format for production
        processors: list[Processor] = [
            *shared_processors,
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    else:
        # Console format for development
        processors = [
            *shared_processors,
            structlog.dev.ConsoleRenderer(colors=True),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Set service name in context
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(service=svc_name)


def get_logger(name: str | None = None, **initial_context: Any) -> structlog.BoundLogger:
    """
    Get a structured logger instance.

    Args:
        name: Logger name (usually module name).
        **initial_context: Initial context values to bind.

    Returns:
        A bound structlog logger.
    """
    logger = structlog.get_logger(name)
    if initial_context:
        logger = logger.bind(**initial_context)
    return logger


class LogContext:
    """Context manager for adding temporary log context."""

    def __init__(self, **context: Any) -> None:
        self._context = context
        self._token: Any = None

    def __enter__(self) -> LogContext:
        self._token = structlog.contextvars.bind_contextvars(**self._context)
        return self

    def __exit__(self, *args: Any) -> None:
        structlog.contextvars.unbind_contextvars(*self._context.keys())


def bind_context(**context: Any) -> None:
    """Bind context variables for all subsequent log calls in this context."""
    structlog.contextvars.bind_contextvars(**context)


def unbind_context(*keys: str) -> None:
    """Unbind context variables."""
    structlog.contextvars.unbind_contextvars(*keys)


def clear_context() -> None:
    """Clear all context variables."""
    structlog.contextvars.clear_contextvars()


# Pre-configured loggers for common components
def get_agent_logger(agent_id: str) -> structlog.BoundLogger:
    """Get a logger bound to an agent context."""
    return get_logger("mongoclaw.agent", agent_id=agent_id)


def get_worker_logger(worker_id: str) -> structlog.BoundLogger:
    """Get a logger bound to a worker context."""
    return get_logger("mongoclaw.worker", worker_id=worker_id)


def get_api_logger() -> structlog.BoundLogger:
    """Get a logger for API operations."""
    return get_logger("mongoclaw.api")
