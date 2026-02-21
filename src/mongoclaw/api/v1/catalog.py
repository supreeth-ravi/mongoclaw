"""Catalog and schema discovery endpoints for UI onboarding."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from fastapi import APIRouter

from mongoclaw.api.dependencies import AgentStoreDep, ApiKeyDep, MongoClientDep

router = APIRouter()


def _value_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _flatten_fields(
    doc: dict[str, Any],
    out: dict[str, dict[str, Any]],
    prefix: str = "",
    depth: int = 0,
    max_depth: int = 3,
) -> None:
    if depth > max_depth:
        return
    for key, value in doc.items():
        path = f"{prefix}.{key}" if prefix else key
        entry = out.setdefault(path, {"count": 0, "types": set(), "example": None})
        entry["count"] += 1
        entry["types"].add(_value_type(value))
        if entry["example"] is None:
            if isinstance(value, (dict, list)):
                entry["example"] = _value_type(value)
            else:
                entry["example"] = value
        if isinstance(value, dict):
            _flatten_fields(value, out, path, depth + 1, max_depth)


@router.get("/overview")
async def get_catalog_overview(
    client: MongoClientDep,
    store: AgentStoreDep,
    _api_key: ApiKeyDep,
) -> dict[str, Any]:
    """Return databases, collections, and watched collection coverage."""
    databases = await client.list_database_names()
    catalog: list[dict[str, Any]] = []

    for db_name in sorted(databases):
        db = client[db_name]
        collections = await db.list_collection_names()
        catalog.append(
            {
                "database": db_name,
                "collections": sorted(collections),
            }
        )

    agents = await store.list(skip=0, limit=2000)
    watched_map: dict[tuple[str, str], list[str]] = defaultdict(list)
    for agent in agents:
        watched_map[(agent.watch.database, agent.watch.collection)].append(agent.id)

    watched = [
        {
            "database": db,
            "collection": coll,
            "agent_ids": sorted(agent_ids),
            "agent_count": len(agent_ids),
        }
        for (db, coll), agent_ids in sorted(watched_map.items())
    ]

    return {
        "databases": catalog,
        "watched_collections": watched,
    }


@router.get("/collection-profile")
async def get_collection_profile(
    database: str,
    collection: str,
    client: MongoClientDep,
    store: AgentStoreDep,
    _api_key: ApiKeyDep,
    sample_size: int = 40,
) -> dict[str, Any]:
    """Return profile for a collection: stats, applied agents, inferred schema."""
    bounded_sample_size = max(1, min(sample_size, 200))
    col = client[database][collection]

    total_docs = await col.count_documents({})
    enriched_docs = await col.count_documents({"_ai_metadata": {"$exists": True}})
    ai_target_docs = await col.count_documents({"ai_triage": {"$exists": True}})

    projection = {
        "_id": 1,
        "_ai_metadata": 1,
        "ai_triage": 1,
    }
    cursor = col.find({}, projection=projection).sort("_id", -1).limit(10)
    recent = [doc async for doc in cursor]

    sample_cursor = col.find({}, projection=None).limit(bounded_sample_size)
    sampled = [doc async for doc in sample_cursor]
    field_summary: dict[str, dict[str, Any]] = {}
    for doc in sampled:
        _flatten_fields(doc, field_summary)

    schema_fields = [
        {
            "path": path,
            "seen_in_samples": info["count"],
            "types": sorted(list(info["types"])),
            "example": info["example"],
        }
        for path, info in sorted(field_summary.items())
    ]

    all_agents = await store.list(skip=0, limit=2000)
    applied_agents = [
        {
            "id": a.id,
            "name": a.name,
            "enabled": a.enabled,
            "operations": [op.value for op in a.watch.operations],
            "model": a.ai.model,
            "provider": a.ai.provider,
            "consistency_mode": a.execution.consistency_mode,
        }
        for a in all_agents
        if a.watch.database == database and a.watch.collection == collection
    ]

    return {
        "database": database,
        "collection": collection,
        "stats": {
            "total_docs": total_docs,
            "enriched_docs": enriched_docs,
            "ai_triage_docs": ai_target_docs,
            "enrichment_pct": round((enriched_docs / total_docs) * 100, 2) if total_docs else 0.0,
        },
        "applied_agents": applied_agents,
        "schema": schema_fields,
        "recent_documents": recent,
        "sample_size": bounded_sample_size,
    }
