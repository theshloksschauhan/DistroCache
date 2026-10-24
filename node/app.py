from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any

import grpc

from distrocache_common.generated import cache_pb2, cache_pb2_grpc
from distrocache_common.grpc_client import GrpcClientPool
from distrocache_common.hashing import ConsistentHashRing

from .membership_client import MembershipClient
from .storage import Storage

logger = logging.getLogger("distrocache.node")


@dataclass(frozen=True)
class NodeConfig:
    node_id: str
    grpc_host: str
    grpc_port: int
    node_address: str
    backend_url: str
    heartbeat_interval: float
    replication_factor: int
    grpc_timeout: float
    ttl_seconds: int
    max_items: int
    storage_path: str


def load_config() -> NodeConfig:
    node_id = os.getenv("NODE_ID", "node-unknown")
    grpc_port = int(os.getenv("GRPC_PORT", "50051"))
    return NodeConfig(
        node_id=node_id,
        # Containers need 0.0.0.0; local runs and tests can bind 127.0.0.1 only.
        grpc_host=os.getenv("GRPC_HOST", "0.0.0.0"),
        grpc_port=grpc_port,
        node_address=os.getenv("NODE_ADDRESS", f"{node_id}:{grpc_port}"),
        backend_url=os.getenv("BACKEND_URL", "http://localhost:8000"),
        heartbeat_interval=float(os.getenv("HEARTBEAT_INTERVAL", "2.0")),
        replication_factor=int(os.getenv("REPLICATION_FACTOR", "3")),
        grpc_timeout=float(os.getenv("GRPC_TIMEOUT", "1.0")),
        ttl_seconds=int(os.getenv("TTL_SECONDS", "0")),
        max_items=int(os.getenv("MAX_ITEMS", "0")),
        storage_path=os.getenv("STORAGE_PATH", "cache.db"),
    )


class CacheNodeService(cache_pb2_grpc.CacheNodeServicer):
    def __init__(self, storage: Storage, node_id: str) -> None:
        self._storage = storage
        self._node_id = node_id

    async def Get(
        self, request: cache_pb2.GetRequest, context: grpc.aio.ServicerContext
    ) -> cache_pb2.GetResponse:
        value = await self._storage.get(request.key)
        if value is None:
            return cache_pb2.GetResponse(found=False)
        return cache_pb2.GetResponse(found=True, value=value)

    async def Put(
        self, request: cache_pb2.PutRequest, context: grpc.aio.ServicerContext
    ) -> cache_pb2.PutResponse:
        try:
            await self._storage.put(request.key, request.value)
            return cache_pb2.PutResponse(ok=True)
        except Exception as exc:
            return cache_pb2.PutResponse(ok=False, error=str(exc))

    async def Delete(
        self, request: cache_pb2.DeleteRequest, context: grpc.aio.ServicerContext
    ) -> cache_pb2.DeleteResponse:
        try:
            ok = await self._storage.delete(request.key)
            return cache_pb2.DeleteResponse(ok=ok)
        except Exception as exc:
            return cache_pb2.DeleteResponse(ok=False, error=str(exc))

    async def ListEntries(
        self, request: cache_pb2.ListEntriesRequest, context: grpc.aio.ServicerContext
    ) -> cache_pb2.ListEntriesResponse:
        entries = await self._storage.list_entries()
        include_values = request.include_values
        response_entries = []
        for key, value, expires_at, version in entries:
            response_entries.append(
                cache_pb2.Entry(
                    key=key,
                    value=value if include_values else b"",
                    expires_at=expires_at or 0.0,
                    version=version,
                )
            )
        return cache_pb2.ListEntriesResponse(entries=response_entries)

    async def GetEntry(
        self, request: cache_pb2.GetEntryRequest, context: grpc.aio.ServicerContext
    ) -> cache_pb2.GetEntryResponse:
        entry = await self._storage.get_entry(request.key)
        if entry is None:
            return cache_pb2.GetEntryResponse(found=False)
        value, expires_at, version = entry
        return cache_pb2.GetEntryResponse(
            found=True,
            entry=cache_pb2.Entry(
                key=request.key,
                value=value,
                expires_at=expires_at or 0.0,
                version=version,
            ),
        )

    async def PutReplica(
        self, request: cache_pb2.PutReplicaRequest, context: grpc.aio.ServicerContext
    ) -> cache_pb2.PutReplicaResponse:
        try:
            applied = await self._storage.upsert_replica(
                request.key,
                request.value,
                request.expires_at if request.expires_at > 0 else None,
                request.version,
            )
            return cache_pb2.PutReplicaResponse(ok=True, applied=applied)
        except Exception as exc:
            return cache_pb2.PutReplicaResponse(ok=False, applied=False, error=str(exc))

    async def Health(
        self, request: cache_pb2.HealthRequest, context: grpc.aio.ServicerContext
    ) -> cache_pb2.HealthResponse:
        return cache_pb2.HealthResponse(node_id=self._node_id, ok=True)


class Node:
    """One storage node: gRPC server, SQLite storage, and the membership/rebalance loops.

    Everything with side effects (opening the database, HTTP clients, tasks) is created
    here rather than at import time, so tests can run several nodes in one process.
    """

    def __init__(
        self,
        config: NodeConfig,
        client_pool: GrpcClientPool | None = None,
    ) -> None:
        self.config = config
        self.storage = Storage(
            db_path=config.storage_path,
            ttl_seconds=config.ttl_seconds if config.ttl_seconds > 0 else None,
            max_items=config.max_items if config.max_items > 0 else None,
        )
        self.membership = MembershipClient(config.backend_url, config.node_id, config.node_address)
        self.client_pool = client_pool or GrpcClientPool()
        self._rebalance_lock = asyncio.Lock()
        # Strong references: the event loop only keeps weak ones, and shutdown must
        # cancel every task the node started.
        self._tasks: set[asyncio.Task[None]] = set()

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def run(self, stop_event: asyncio.Event) -> None:
        await self.storage.initialize()
        server = grpc.aio.server()
        cache_pb2_grpc.add_CacheNodeServicer_to_server(
            CacheNodeService(self.storage, self.config.node_id), server
        )
        server.add_insecure_port(f"{self.config.grpc_host}:{self.config.grpc_port}")
        await server.start()
        logger.info(
            "node %s listening on %s:%s",
            self.config.node_id,
            self.config.grpc_host,
            self.config.grpc_port,
        )
        try:
            await self._register()
            self._spawn(self._heartbeat_loop())
            await stop_event.wait()
        finally:
            await self._shutdown(server)

    async def _register(self) -> None:
        try:
            key_count = await self.storage.count()
            await self.membership.register(key_count)
        except Exception as exc:
            logger.warning("membership register failed: %s", exc)

    async def _shutdown(self, server: grpc.aio.Server) -> None:
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await self.membership.deregister()
        except Exception as exc:
            logger.warning("membership deregister failed: %s", exc)
        await self.membership.close()
        await self.client_pool.close()
        await server.stop(grace=2)
        self.storage.close()
        logger.info("node %s stopped", self.config.node_id)

    async def _heartbeat_loop(self) -> None:
        last_ring_version = -1
        while True:
            try:
                key_count = await self.storage.count()
                ring_version, nodes = await self.membership.heartbeat(key_count)
                if ring_version != last_ring_version:
                    last_ring_version = ring_version
                    if nodes:
                        self._spawn(self._rebalance(nodes))
            except Exception as exc:
                logger.warning("heartbeat failed: %s", exc)
            await asyncio.sleep(self.config.heartbeat_interval)

    async def _rebalance(self, nodes: list[str]) -> None:
        if not nodes:
            return
        async with self._rebalance_lock:
            start = time.perf_counter()
            keys_moved, replicas_restored = await self._rebalance_once(nodes)
            duration = time.perf_counter() - start
            await self.membership.report_rebalance(duration, keys_moved, replicas_restored)

    async def _rebalance_once(self, nodes: list[str]) -> tuple[int, int]:
        ring = ConsistentHashRing(nodes)
        entries = await self.storage.list_entries()
        keys_moved = 0
        replicas_restored = 0
        for key, value, expires_at, version in entries:
            replicas = ring.get_nodes(key, self.config.replication_factor)
            if not replicas:
                continue
            applied_any = False
            for replica in replicas:
                if replica == self.config.node_address:
                    continue
                try:
                    stub = await self.client_pool.get_stub(replica)
                    response = await stub.PutReplica(
                        cache_pb2.PutReplicaRequest(
                            key=key,
                            value=value,
                            expires_at=expires_at or 0.0,
                            version=version,
                        ),
                        timeout=self.config.grpc_timeout,
                    )
                    if response.ok and response.applied:
                        replicas_restored += 1
                        applied_any = True
                except Exception:
                    continue
            if self.config.node_address not in replicas and applied_any:
                removed = await self.storage.delete(key)
                if removed:
                    keys_moved += 1
        return keys_moved, replicas_restored


def _install_stop_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    if sys.platform == "win32":
        # The Windows event loop has no add_signal_handler. A plain handler runs on the main
        # thread between bytecodes, so hand the stop over to the loop thread-safely.
        # CTRL_BREAK_EVENT (what supervisors and the test harness send) arrives as SIGBREAK.
        def _on_signal(signum: int, frame: object) -> None:
            loop.call_soon_threadsafe(stop_event.set)

        signal.signal(signal.SIGBREAK, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)
        return
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)


async def _serve(config: NodeConfig) -> None:
    stop_event = asyncio.Event()
    _install_stop_handlers(stop_event)
    await Node(config).run(stop_event)


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    asyncio.run(_serve(load_config()))


if __name__ == "__main__":
    main()
