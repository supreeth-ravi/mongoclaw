"""Serialization utilities for queue messages."""

from __future__ import annotations

import json
from typing import Any

import msgpack

from mongoclaw.dispatcher.work_item import WorkItem


class SerializationFormat:
    """Supported serialization formats."""

    JSON = "json"
    MSGPACK = "msgpack"


def serialize_work_item(
    work_item: WorkItem,
    format: str = SerializationFormat.JSON,
) -> str | bytes:
    """
    Serialize a work item for queue storage.

    Args:
        work_item: The work item to serialize.
        format: Serialization format ("json" or "msgpack").

    Returns:
        Serialized data as string (JSON) or bytes (msgpack).
    """
    data = work_item.to_queue_data()

    if format == SerializationFormat.MSGPACK:
        return msgpack.packb(data, use_bin_type=True)

    return json.dumps(data)


def deserialize_work_item(
    data: str | bytes,
    format: str | None = None,
) -> WorkItem:
    """
    Deserialize a work item from queue data.

    Args:
        data: Serialized data.
        format: Serialization format (auto-detected if None).

    Returns:
        Deserialized WorkItem.

    Raises:
        ValueError: If deserialization fails.
    """
    # Auto-detect format
    if format is None:
        if isinstance(data, bytes):
            format = SerializationFormat.MSGPACK
        else:
            format = SerializationFormat.JSON

    try:
        if format == SerializationFormat.MSGPACK:
            if isinstance(data, str):
                data = data.encode()
            parsed = msgpack.unpackb(data, raw=False)
        else:
            if isinstance(data, bytes):
                data = data.decode()
            parsed = json.loads(data)

        return WorkItem.from_queue_data(parsed)

    except Exception as e:
        raise ValueError(f"Failed to deserialize work item: {e}")


def serialize_any(
    obj: Any,
    format: str = SerializationFormat.JSON,
) -> str | bytes:
    """
    Serialize any object for queue storage.

    Args:
        obj: The object to serialize.
        format: Serialization format.

    Returns:
        Serialized data.
    """
    if format == SerializationFormat.MSGPACK:
        return msgpack.packb(obj, use_bin_type=True)

    return json.dumps(obj)


def deserialize_any(
    data: str | bytes,
    format: str | None = None,
) -> Any:
    """
    Deserialize any object from queue data.

    Args:
        data: Serialized data.
        format: Serialization format (auto-detected if None).

    Returns:
        Deserialized object.
    """
    if format is None:
        if isinstance(data, bytes):
            format = SerializationFormat.MSGPACK
        else:
            format = SerializationFormat.JSON

    if format == SerializationFormat.MSGPACK:
        if isinstance(data, str):
            data = data.encode()
        return msgpack.unpackb(data, raw=False)

    if isinstance(data, bytes):
        data = data.decode()
    return json.loads(data)
