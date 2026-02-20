"""Health check endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Response

from mongoclaw import __version__
from mongoclaw.api.dependencies import MongoClientDep, SettingsDep
from mongoclaw.core.types import HealthStatus
from mongoclaw.observability.metrics import get_metrics_collector

router = APIRouter()


@router.get("/health")
async def health_check() -> dict[str, str]:
    """Basic health check."""
    return {"status": "healthy"}


@router.get("/health/live")
async def liveness_check() -> dict[str, str]:
    """Kubernetes liveness probe."""
    return {"status": "alive"}


@router.get("/health/ready")
async def readiness_check(
    client: MongoClientDep,
) -> dict[str, Any]:
    """
    Kubernetes readiness probe.

    Checks that all dependencies are available.
    """
    checks: dict[str, dict[str, Any]] = {}

    # Check MongoDB
    try:
        await client.admin.command("ping")
        checks["mongodb"] = {"status": "healthy"}
    except Exception as e:
        checks["mongodb"] = {"status": "unhealthy", "error": str(e)}

    # Determine overall status
    all_healthy = all(
        c.get("status") == "healthy" for c in checks.values()
    )

    return {
        "status": "ready" if all_healthy else "not_ready",
        "checks": checks,
    }


@router.get("/health/detailed")
async def detailed_health_check(
    client: MongoClientDep,
    settings: SettingsDep,
) -> dict[str, Any]:
    """Detailed health check with all component statuses."""
    components: dict[str, dict[str, Any]] = {}

    # MongoDB
    try:
        result = await client.admin.command("ping")
        components["mongodb"] = {
            "status": HealthStatus.HEALTHY.value,
            "details": {"ping": result},
        }
    except Exception as e:
        components["mongodb"] = {
            "status": HealthStatus.UNHEALTHY.value,
            "error": str(e),
        }

    # TODO: Add Redis check when queue is initialized

    # Determine overall status
    statuses = [c.get("status") for c in components.values()]
    if all(s == HealthStatus.HEALTHY.value for s in statuses):
        overall = HealthStatus.HEALTHY
    elif any(s == HealthStatus.UNHEALTHY.value for s in statuses):
        overall = HealthStatus.UNHEALTHY
    else:
        overall = HealthStatus.DEGRADED

    return {
        "status": overall.value,
        "version": __version__,
        "environment": settings.environment,
        "components": components,
    }


@router.get("/metrics")
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    collector = get_metrics_collector()
    content = collector.get_metrics()

    return Response(
        content=content,
        media_type="text/plain; charset=utf-8",
    )


@router.get("/version")
async def version() -> dict[str, str]:
    """Get application version."""
    return {
        "version": __version__,
        "name": "mongoclaw",
    }
