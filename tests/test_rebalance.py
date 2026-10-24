import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest

from distrocache_common.generated import cache_pb2
from node.app import Node, NodeConfig


class FakeStub:
    """Accepts every PutReplica and runs `on_push` first, to inject concurrent writes."""

    def __init__(self, on_push: Callable[[cache_pb2.PutReplicaRequest], Awaitable[None]]) -> None:
        self.on_push = on_push
        self.pushed: list[cache_pb2.PutReplicaRequest] = []

    async def PutReplica(
        self, request: cache_pb2.PutReplicaRequest, timeout: float
    ) -> cache_pb2.PutReplicaResponse:
        self.pushed.append(request)
        await self.on_push(request)
        return cache_pb2.PutReplicaResponse(ok=True, applied=True)


class FakePool:
    def __init__(self, stub: FakeStub) -> None:
        self.stub = stub

    async def get_stub(self, target: str) -> FakeStub:
        return self.stub

    async def close(self) -> None:
        pass


def _config(tmp_path: Path) -> NodeConfig:
    return NodeConfig(
        node_id="leaving",
        grpc_host="127.0.0.1",
        grpc_port=1,
        node_address="127.0.0.1:1",
        backend_url="http://127.0.0.1:1",
        heartbeat_interval=1.0,
        replication_factor=2,
        grpc_timeout=0.5,
        ttl_seconds=0,
        max_items=0,
        storage_path=str(tmp_path / "cache.db"),
    )


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Stage 7: rebalance deletes the local row without checking it is still the pushed one",
)
def test_rebalance_keeps_a_write_that_lands_during_the_push(tmp_path: Path) -> None:
    async def scenario() -> None:
        node: Node

        async def write_during_push(request: cache_pb2.PutReplicaRequest) -> None:
            # A client write reaches this node after the snapshot but before the delete.
            if request.value == b"old":
                await node.storage.put("moving", b"new")

        stub = FakeStub(write_during_push)
        node = Node(_config(tmp_path), client_pool=FakePool(stub))  # type: ignore[arg-type]
        try:
            await node.storage.initialize()
            await node.storage.put("moving", b"old")

            # This node is not an owner in the new ring, so it hands the key off and deletes it.
            await node._rebalance_once(["127.0.0.1:2", "127.0.0.1:3"])

            local = await node.storage.get_entry("moving")
        finally:
            await node.membership.close()
            node.storage.close()

        pushed_values = [request.value for request in stub.pushed]
        survived = (local is not None and local[0] == b"new") or b"new" in pushed_values
        assert survived, (
            f"the concurrent write was lost: local copy {local}, pushed values {pushed_values}"
        )

    asyncio.run(scenario())
