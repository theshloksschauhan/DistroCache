import asyncio
import dataclasses
import socket
from pathlib import Path

import grpc
import pytest

from node.app import Node, NodeConfig


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _config(tmp_path: Path) -> NodeConfig:
    port = _free_port()
    return NodeConfig(
        node_id="node-test",
        grpc_host="127.0.0.1",
        grpc_port=port,
        node_address=f"127.0.0.1:{port}",
        # Nothing listens here, so register/heartbeat fail fast and are only logged.
        backend_url=f"http://127.0.0.1:{_free_port()}",
        heartbeat_interval=0.05,
        replication_factor=3,
        write_quorum=2,
        grpc_timeout=0.5,
        max_items=0,
        storage_path=str(tmp_path / "cache.db"),
        tombstone_gc_seconds=3600,
    )


def test_stop_cancels_every_background_task(tmp_path: Path) -> None:
    async def scenario() -> None:
        node = Node(_config(tmp_path))
        stop_event = asyncio.Event()
        run_task = asyncio.create_task(node.run(stop_event))

        for _ in range(200):
            if node._tasks:
                break
            await asyncio.sleep(0.01)
        assert node._tasks, "heartbeat loop never started"

        # Stands in for a rebalance that is still running when the node is asked to stop.
        node._spawn(asyncio.sleep(3600))
        started = set(node._tasks)

        stop_event.set()
        await asyncio.wait_for(run_task, timeout=15)

        assert all(task.cancelled() for task in started), "a background task outlived stop"
        assert not node._tasks

    asyncio.run(scenario())


def test_start_fails_when_port_is_held_by_another_grpc_server(tmp_path: Path) -> None:
    async def scenario() -> None:
        # Default options: on Linux this server has SO_REUSEPORT, like another node would.
        holder = grpc.aio.server()
        port = holder.add_insecure_port("127.0.0.1:0")
        await holder.start()
        config = dataclasses.replace(
            _config(tmp_path), grpc_port=port, node_address=f"127.0.0.1:{port}"
        )
        node = Node(config)
        stop_event = asyncio.Event()
        run_task = asyncio.create_task(node.run(stop_event))
        try:
            done, _ = await asyncio.wait({run_task}, timeout=10)
            if not done:
                stop_event.set()
                await run_task
            assert done, "node started on a port another gRPC server is listening on"
            with pytest.raises(RuntimeError, match="Failed to bind"):
                run_task.result()
        finally:
            await holder.stop(grace=None)
            await node.membership.close()
            node.storage.close()

    asyncio.run(scenario())
