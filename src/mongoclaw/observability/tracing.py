"""OpenTelemetry tracing integration."""

from __future__ import annotations

import functools
from contextlib import contextmanager
from typing import Any, Callable, TypeVar

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes

from mongoclaw.core.config import get_settings
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


def configure_tracing(
    service_name: str | None = None,
    endpoint: str | None = None,
    sample_rate: float = 0.1,
) -> None:
    """
    Configure OpenTelemetry tracing.

    Args:
        service_name: Service name for traces.
        endpoint: OTLP endpoint for exporting.
        sample_rate: Sampling rate (0.0-1.0).
    """
    settings = get_settings()

    if not settings.observability.tracing_enabled:
        logger.info("Tracing disabled")
        return

    svc_name = service_name or settings.observability.service_name
    otlp_endpoint = endpoint or settings.observability.tracing_endpoint

    # Create resource
    resource = Resource.create({
        ResourceAttributes.SERVICE_NAME: svc_name,
        ResourceAttributes.SERVICE_VERSION: "0.1.0",
    })

    # Create tracer provider
    provider = TracerProvider(resource=resource)

    # Add OTLP exporter if endpoint configured
    if otlp_endpoint:
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

            exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
            processor = BatchSpanProcessor(exporter)
            provider.add_span_processor(processor)

            logger.info("Tracing configured", endpoint=otlp_endpoint)

        except ImportError:
            logger.warning("OTLP exporter not available")

    # Set global tracer provider
    trace.set_tracer_provider(provider)


def get_tracer(name: str = "mongoclaw") -> trace.Tracer:
    """Get a tracer instance."""
    return trace.get_tracer(name)


@contextmanager
def span(
    name: str,
    attributes: dict[str, Any] | None = None,
    tracer_name: str = "mongoclaw",
):
    """
    Context manager for creating a span.

    Args:
        name: Span name.
        attributes: Span attributes.
        tracer_name: Tracer name.

    Yields:
        The span.
    """
    tracer = get_tracer(tracer_name)

    with tracer.start_as_current_span(name) as current_span:
        if attributes:
            for key, value in attributes.items():
                current_span.set_attribute(key, value)

        try:
            yield current_span
        except Exception as e:
            current_span.set_status(
                trace.Status(trace.StatusCode.ERROR, str(e))
            )
            current_span.record_exception(e)
            raise


def traced(
    name: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for tracing functions.

    Args:
        name: Span name (defaults to function name).
        attributes: Additional span attributes.

    Returns:
        Decorator function.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        span_name = name or func.__name__

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> T:
            with span(span_name, attributes):
                return await func(*args, **kwargs)

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> T:
            with span(span_name, attributes):
                return func(*args, **kwargs)

        if asyncio_iscoroutinefunction(func):
            return async_wrapper  # type: ignore
        return sync_wrapper  # type: ignore

    return decorator


def asyncio_iscoroutinefunction(func: Any) -> bool:
    """Check if function is a coroutine function."""
    import asyncio
    return asyncio.iscoroutinefunction(func)


def set_span_attribute(key: str, value: Any) -> None:
    """Set an attribute on the current span."""
    current_span = trace.get_current_span()
    if current_span:
        current_span.set_attribute(key, value)


def set_span_status(status_code: trace.StatusCode, description: str = "") -> None:
    """Set status on the current span."""
    current_span = trace.get_current_span()
    if current_span:
        current_span.set_status(trace.Status(status_code, description))


def record_exception(exception: Exception) -> None:
    """Record an exception on the current span."""
    current_span = trace.get_current_span()
    if current_span:
        current_span.record_exception(exception)
        current_span.set_status(
            trace.Status(trace.StatusCode.ERROR, str(exception))
        )


def add_event(name: str, attributes: dict[str, Any] | None = None) -> None:
    """Add an event to the current span."""
    current_span = trace.get_current_span()
    if current_span:
        current_span.add_event(name, attributes=attributes)


def get_trace_context() -> dict[str, str]:
    """Get the current trace context for propagation."""
    current_span = trace.get_current_span()
    if not current_span:
        return {}

    context = current_span.get_span_context()
    if not context.is_valid:
        return {}

    return {
        "trace_id": format(context.trace_id, "032x"),
        "span_id": format(context.span_id, "016x"),
    }


class SpanContextCarrier:
    """
    Carrier for span context propagation.

    Used to pass trace context across process boundaries.
    """

    def __init__(self) -> None:
        self._context: dict[str, str] = {}

    def inject(self) -> None:
        """Inject current span context."""
        self._context = get_trace_context()

    def extract(self) -> dict[str, str]:
        """Extract stored context."""
        return self._context

    def get_trace_id(self) -> str | None:
        """Get trace ID from context."""
        return self._context.get("trace_id")

    def get_span_id(self) -> str | None:
        """Get span ID from context."""
        return self._context.get("span_id")
