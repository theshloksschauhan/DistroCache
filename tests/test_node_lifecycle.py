import asyncio
import socket
from pathlib import Path

from node.app import Node, NodeConfig


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _config(tmp_path: Path) -> NodeConfig:
    port = _free_port()
    return NodeConfig(
        node_id="node-test",
        grpc_port=port,
        node_address=f"127.0.0.1:{port}",
        # Nothing listens here, so register/heartbeat fail fast and are only logged.
        backend_url=f"http://127.0.0.1:{_free_port()}",
        heartbeat_interval=0.05,
        replication_factor=3,
        grpc_timeout=0.5,
        ttl_seconds=0,
        max_items=0,
        storage_path=str(tmp_path / "cache.db"),
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
