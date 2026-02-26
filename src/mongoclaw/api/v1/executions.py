"""Execution history endpoints."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from mongoclaw.api.dependencies import ApiKeyDep, MongoClientDep, PaginationDep, SettingsDep

router = APIRouter()


class ExecutionRecord(BaseModel):
    """Execution record model."""

    id: str
    agent_id: str
    document_id: str
    status: str
    lifecycle_state: str | None = None
    reason: str | None = None
    written: bool | None = None
    started_at: datetime
    completed_at: datetime | None = None
    duration_ms: float | None = None
    ai_response: dict[str, Any] | None = None
    error: str | None = None
    attempt: int = 1


class ExecutionListResponse(BaseModel):
    """Response for execution listing."""

    executions: list[dict[str, Any]]
    total: int
    skip: int
    limit: int


@router.get("")
async def list_executions(
    client: MongoClientDep,
    settings: SettingsDep,
    pagination: PaginationDep,
    _api_key: ApiKeyDep,
    agent_id: str | None = None,
    status_filter: str | None = None,
) -> ExecutionListResponse:
    """List execution history."""
    db = client[settings.mongodb.database]
    collection = db[settings.mongodb.executions_collection]

    query: dict[str, Any] = {}
    if agent_id:
        query["agent_id"] = agent_id
    if status_filter:
        query["status"] = status_filter

    cursor = collection.find(query).skip(pagination.skip).limit(pagination.limit)
    cursor = cursor.sort("started_at", -1)

    executions = []
    async for doc in cursor:
        doc["id"] = str(doc.pop("_id"))
        executions.append(doc)

    total = await collection.count_documents(query)

    return ExecutionListResponse(
        executions=executions,
        total=total,
        skip=pagination.skip,
        limit=pagination.limit,
    )


@router.get("/agent/{agent_id}")
async def get_agent_executions(
    agent_id: str,
    client: MongoClientDep,
    settings: SettingsDep,
    pagination: PaginationDep,
    _api_key: ApiKeyDep,
) -> ExecutionListResponse:
    """Get executions for a specific agent."""
    db = client[settings.mongodb.database]
    collection = db[settings.mongodb.executions_collection]

    query = {"agent_id": agent_id}

    cursor = collection.find(query).skip(pagination.skip).limit(pagination.limit)
    cursor = cursor.sort("started_at", -1)

    executions = []
    async for doc in cursor:
        doc["id"] = str(doc.pop("_id"))
        executions.append(doc)

    total = await collection.count_documents(query)

    return ExecutionListResponse(
        executions=executions,
        total=total,
        skip=pagination.skip,
        limit=pagination.limit,
    )


@router.get("/stats")
async def get_execution_stats(
    client: MongoClientDep,
    settings: SettingsDep,
    _api_key: ApiKeyDep,
    agent_id: str | None = None,
    hours: int = 24,
) -> dict[str, Any]:
    """Get execution statistics."""
    db = client[settings.mongodb.database]
    collection = db[settings.mongodb.executions_collection]

    bounded_hours = max(1, min(hours, 24 * 30))
    cutoff = datetime.utcnow() - timedelta(hours=bounded_hours)

    match_stage: dict[str, Any] = {"started_at": {"$gte": cutoff}}
    if agent_id:
        match_stage["agent_id"] = agent_id

    pipeline = [
        {"$match": match_stage},
        {
            "$group": {
                "_id": "$status",
                "count": {"$sum": 1},
                "avg_duration_ms": {"$avg": "$duration_ms"},
            }
        },
    ]

    cursor = collection.aggregate(pipeline)
    results = [doc async for doc in cursor]

    stats: dict[str, Any] = {
        "period_hours": bounded_hours,
        "by_status": {},
        "total": 0,
    }

    for result in results:
        status_name = result["_id"]
        stats["by_status"][status_name] = {
            "count": result["count"],
            "avg_duration_ms": round(result.get("avg_duration_ms", 0), 2),
        }
        stats["total"] += result["count"]

    return stats


@router.get("/{execution_id}")
async def get_execution(
    execution_id: str,
    client: MongoClientDep,
    settings: SettingsDep,
    _api_key: ApiKeyDep,
) -> dict[str, Any]:
    """Get a specific execution record."""
    from bson import ObjectId

    db = client[settings.mongodb.database]
    collection = db[settings.mongodb.executions_collection]

    try:
        doc = await collection.find_one({"_id": ObjectId(execution_id)})
    except Exception:
        doc = await collection.find_one({"_id": execution_id})

    if not doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Execution '{execution_id}' not found",
        )

    doc["id"] = str(doc.pop("_id"))
    return doc
