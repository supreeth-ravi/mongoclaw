"""FastAPI middleware for observability."""

from __future__ import annotations

import time
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from mongoclaw.observability.logging import get_logger, bind_context, unbind_context
from mongoclaw.observability.metrics import get_metrics_collector
from mongoclaw.observability.tracing import span, set_span_attribute

logger = get_logger(__name__)


class MetricsMiddleware(BaseHTTPMiddleware):
    """
    Middleware for collecting HTTP metrics.

    Records request count and latency to Prometheus.
    """

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Response],
    ) -> Response:
        start_time = time.perf_counter()

        response = await call_next(request)

        duration = time.perf_counter() - start_time

        # Get normalized endpoint path
        endpoint = self._get_endpoint(request)

        # Record metrics
        collector = get_metrics_collector()
        collector.record_http_request(
            method=request.method,
            endpoint=endpoint,
            status=response.status_code,
            duration_seconds=duration,
        )

        return response

    def _get_endpoint(self, request: Request) -> str:
        """Get normalized endpoint path."""
        # Try to get the matched route
        if hasattr(request, "scope") and "route" in request.scope:
            route = request.scope["route"]
            if hasattr(route, "path"):
                return route.path

        # Fall back to URL path
        return request.url.path


class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware for request logging.

    Logs requests with structured context.
    """

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Response],
    ) -> Response:
        # Generate or extract request ID
        request_id = request.headers.get("X-Request-ID", str(time.time_ns()))

        # Bind context for all logs in this request
        bind_context(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
        )

        start_time = time.perf_counter()

        try:
            response = await call_next(request)

            duration = time.perf_counter() - start_time

            logger.info(
                "Request completed",
                status_code=response.status_code,
                duration_ms=round(duration * 1000, 2),
            )

            # Add request ID to response headers
            response.headers["X-Request-ID"] = request_id

            return response

        except Exception as e:
            duration = time.perf_counter() - start_time
            logger.exception(
                "Request failed",
                error=str(e),
                duration_ms=round(duration * 1000, 2),
            )
            raise

        finally:
            unbind_context("request_id", "method", "path")


class TracingMiddleware(BaseHTTPMiddleware):
    """
    Middleware for distributed tracing.

    Creates spans for each request and propagates context.
    """

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Response],
    ) -> Response:
        span_name = f"{request.method} {self._get_endpoint(request)}"

        with span(span_name) as current_span:
            # Set span attributes
            set_span_attribute("http.method", request.method)
            set_span_attribute("http.url", str(request.url))
            set_span_attribute("http.target", request.url.path)

            if request.client:
                set_span_attribute("http.client_ip", request.client.host)

            response = await call_next(request)

            # Set response attributes
            set_span_attribute("http.status_code", response.status_code)

            return response

    def _get_endpoint(self, request: Request) -> str:
        """Get endpoint path for span name."""
        if hasattr(request, "scope") and "route" in request.scope:
            route = request.scope["route"]
            if hasattr(route, "path"):
                return route.path
        return request.url.path


class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    """
    Middleware for consistent error handling.

    Catches exceptions and returns appropriate error responses.
    """

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Response],
    ) -> Response:
        try:
            return await call_next(request)

        except Exception as e:
            from fastapi.responses import JSONResponse

            logger.exception(
                "Unhandled exception",
                error=str(e),
                path=request.url.path,
            )

            # Determine status code
            status_code = getattr(e, "status_code", 500)

            return JSONResponse(
                status_code=status_code,
                content={
                    "error": type(e).__name__,
                    "message": str(e),
                    "path": request.url.path,
                },
            )


def setup_middleware(app):
    """
    Set up all observability middleware on a FastAPI app.

    Args:
        app: FastAPI application instance.
    """
    # Add middleware in reverse order (first added = outermost)
    app.add_middleware(ErrorHandlerMiddleware)
    app.add_middleware(TracingMiddleware)
    app.add_middleware(MetricsMiddleware)
    app.add_middleware(LoggingMiddleware)

    logger.info("Observability middleware configured")
