"""Webhook endpoints for external integrations."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

from mongoclaw.api.dependencies import ApiKeyDep
from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()


class WebhookPayload(BaseModel):
    """Generic webhook payload."""

    event_type: str
    data: dict[str, Any]
    timestamp: str | None = None


class WebhookResponse(BaseModel):
    """Webhook response."""

    received: bool
    message: str | None = None


@router.post("/github")
async def github_webhook(
    request: Request,
) -> WebhookResponse:
    """
    GitHub webhook endpoint.

    Handles GitHub events for triggering agent reloads on config changes.
    """
    event_type = request.headers.get("X-GitHub-Event", "unknown")
    delivery_id = request.headers.get("X-GitHub-Delivery", "")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON payload",
        )

    logger.info(
        "Received GitHub webhook",
        event_type=event_type,
        delivery_id=delivery_id,
    )

    # Handle push events that might contain config changes
    if event_type == "push":
        # Check if agent configs were modified
        commits = payload.get("commits", [])
        config_changed = any(
            any("agents" in f for f in c.get("modified", []) + c.get("added", []))
            for c in commits
        )

        if config_changed:
            logger.info("Agent configs may have changed, triggering reload")
            # TODO: Trigger agent reload

    return WebhookResponse(
        received=True,
        message=f"Processed {event_type} event",
    )


@router.post("/slack")
async def slack_webhook(
    payload: WebhookPayload,
    _api_key: ApiKeyDep,
) -> WebhookResponse:
    """
    Slack webhook endpoint.

    Handles Slack events for notifications and commands.
    """
    logger.info(
        "Received Slack webhook",
        event_type=payload.event_type,
    )

    return WebhookResponse(received=True)


@router.post("/custom")
async def custom_webhook(
    payload: WebhookPayload,
    _api_key: ApiKeyDep,
) -> WebhookResponse:
    """
    Custom webhook endpoint.

    Generic endpoint for custom integrations.
    """
    logger.info(
        "Received custom webhook",
        event_type=payload.event_type,
    )

    # Process based on event type
    if payload.event_type == "reload_agents":
        # TODO: Trigger agent reload
        return WebhookResponse(
            received=True,
            message="Agent reload triggered",
        )

    if payload.event_type == "pause_processing":
        # TODO: Pause worker pool
        return WebhookResponse(
            received=True,
            message="Processing paused",
        )

    if payload.event_type == "resume_processing":
        # TODO: Resume worker pool
        return WebhookResponse(
            received=True,
            message="Processing resumed",
        )

    return WebhookResponse(
        received=True,
        message=f"Event '{payload.event_type}' received",
    )


@router.post("/alert")
async def alert_webhook(
    payload: WebhookPayload,
    _api_key: ApiKeyDep,
) -> WebhookResponse:
    """
    Alert webhook endpoint.

    Receives alerts from monitoring systems.
    """
    logger.warning(
        "Received alert webhook",
        event_type=payload.event_type,
        data=payload.data,
    )

    # Process alert
    alert_name = payload.data.get("alert_name", "unknown")
    severity = payload.data.get("severity", "unknown")

    # TODO: Take action based on alert
    if severity == "critical":
        logger.error(
            "Critical alert received",
            alert_name=alert_name,
        )

    return WebhookResponse(
        received=True,
        message=f"Alert '{alert_name}' processed",
    )
