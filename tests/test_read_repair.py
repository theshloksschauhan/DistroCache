"""Read repair pushes the winning version to replicas that answered stale or missing."""

from __future__ import annotations

import asyncio
from typing import Any

from backend.app import Coordinator, Settings, create_app, get_key
from distrocache_common.generated import cache_pb2


def _coordinator(read_repair_enabled: bool) -> Coordinator:
    app = create_app(
        Settings(
            node_addresses=["a", "b", "c"],
            replication_factor=3,
            read_quorum=2,
            write_quorum=2,
            read_repair_enabled=read_repair_enabled,
            retry_attempts=1,
            retry_backoff=0,
        )
    )
    coordinator: Coordinator = app.state.coordinator
    return coordinator


def _version(ms: int) -> cache_pb2.Version:
    return cache_pb2.Version(physical_ms=ms, logical=0, coordinator_id="c")


async def _get_then_drain(coordinator: Coordinator, key: str) -> Any:
    result = await get_key(key, coordinator)
    # The repair runs as a detached background task; wait it out inside this loop.
    if coordinator._background:
        await asyncio.gather(*coordinator._background, return_exceptions=True)
    return result


def test_read_repair_pushes_the_winner_to_stale_and_missing_replicas() -> None:
    coordinator = _coordinator(read_repair_enabled=True)

    async def fake_get(node: str, key: str) -> cache_pb2.GetResponse:
        if node == "a":  # the freshest
            return cache_pb2.GetResponse(found=True, value=b"v2", version=_version(2))
        if node == "b":  # stale
            return cache_pb2.GetResponse(found=True, value=b"v1", version=_version(1))
        return cache_pb2.GetResponse(found=False)  # c is missing

    pushes: list[tuple[str, bytes, bool]] = []

    async def fake_put_replica(
        node: str, key: str, value: bytes, expires_at: float, version: Any, tombstone: bool
    ) -> cache_pb2.PutReplicaResponse:
        pushes.append((node, value, tombstone))
        return cache_pb2.PutReplicaResponse(status=cache_pb2.WriteStatus.APPLIED)

    coordinator.grpc_get = fake_get  # type: ignore[method-assign]
    coordinator.grpc_put_replica = fake_put_replica  # type: ignore[method-assign]

    result = asyncio.run(_get_then_drain(coordinator, "k"))

    assert result["value"] == "v2"
    # The stale replica (b) and the missing one (c) are repaired; the winner (a) is not pushed.
    assert {node for node, _, _ in pushes} == {"b", "c"}
    assert all(value == b"v2" and tombstone is False for _, value, tombstone in pushes)


def test_read_repair_propagates_a_tombstone() -> None:
    coordinator = _coordinator(read_repair_enabled=True)

    async def fake_get(node: str, key: str) -> cache_pb2.GetResponse:
        if node == "a":  # a tombstone with the highest version
            return cache_pb2.GetResponse(tombstone=True, version=_version(3))
        return cache_pb2.GetResponse(found=True, value=b"v1", version=_version(1))

    pushes: list[tuple[str, bool]] = []

    async def fake_put_replica(
        node: str, key: str, value: bytes, expires_at: float, version: Any, tombstone: bool
    ) -> cache_pb2.PutReplicaResponse:
        pushes.append((node, tombstone))
        return cache_pb2.PutReplicaResponse(status=cache_pb2.WriteStatus.APPLIED)

    coordinator.grpc_get = fake_get  # type: ignore[method-assign]
    coordinator.grpc_put_replica = fake_put_replica  # type: ignore[method-assign]

    async def run() -> int:
        try:
            await get_key("k", coordinator)
            status = 200
        except Exception as exc:
            status = getattr(exc, "status_code", 0)
        if coordinator._background:
            await asyncio.gather(*coordinator._background, return_exceptions=True)
        return status

    status = asyncio.run(run())

    assert status == 404  # a winning tombstone reads as a miss
    # The two live replicas are repaired with the tombstone.
    assert {node for node, _ in pushes} == {"b", "c"}
    assert all(tombstone for _, tombstone in pushes)


def test_read_repair_off_pushes_nothing() -> None:
    coordinator = _coordinator(read_repair_enabled=False)

    async def fake_get(node: str, key: str) -> cache_pb2.GetResponse:
        if node == "a":
            return cache_pb2.GetResponse(found=True, value=b"v2", version=_version(2))
        return cache_pb2.GetResponse(found=True, value=b"v1", version=_version(1))

    pushes: list[str] = []

    async def fake_put_replica(
        node: str, key: str, value: bytes, expires_at: float, version: Any, tombstone: bool
    ) -> cache_pb2.PutReplicaResponse:
        pushes.append(node)
        return cache_pb2.PutReplicaResponse(status=cache_pb2.WriteStatus.APPLIED)

    coordinator.grpc_get = fake_get  # type: ignore[method-assign]
    coordinator.grpc_put_replica = fake_put_replica  # type: ignore[method-assign]

    result = asyncio.run(_get_then_drain(coordinator, "k"))

    assert result["value"] == "v2"
    assert pushes == []
