from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import time
from typing import Optional

import grpc

from .generated import cache_pb2, cache_pb2_grpc
from .grpc_client import GrpcClientPool
from .hashing import ConsistentHashRing
from .membership_client import MembershipClient
from .storage import Storage

NODE_ID = os.getenv("NODE_ID", "node-unknown")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
NODE_ADDRESS = os.getenv("NODE_ADDRESS", f"{NODE_ID}:{GRPC_PORT}")
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "2.0"))
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "3"))
GRPC_TIMEOUT = float(os.getenv("GRPC_TIMEOUT", "1.0"))
TTL_SECONDS = int(os.getenv("TTL_SECONDS", "0"))
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "0"))
STORAGE_PATH = os.getenv("STORAGE_PATH", "cache.db")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("distrocache.node")

storage = Storage(
    db_path=STORAGE_PATH,
    ttl_seconds=TTL_SECONDS if TTL_SECONDS > 0 else None,
    max_items=MAX_ITEMS if MAX_ITEMS > 0 else None,
)
membership = MembershipClient(BACKEND_URL, NODE_ID, NODE_ADDRESS)
client_pool = GrpcClientPool()
rebalance_lock = asyncio.Lock()
heartbeat_task: Optional[asyncio.Task[None]] = None


class CacheNodeService(cache_pb2_grpc.CacheNodeServicer):
    async def Get(self, request: cache_pb2.GetRequest, context: grpc.aio.ServicerContext) -> cache_pb2.GetResponse:
        value = await storage.get(request.key)
        if value is None:
            return cache_pb2.GetResponse(found=False)
        return cache_pb2.GetResponse(found=True, value=value)

    async def Put(self, request: cache_pb2.PutRequest, context: grpc.aio.ServicerContext) -> cache_pb2.PutResponse:
        try:
            await storage.put(request.key, request.value)
            return cache_pb2.PutResponse(ok=True)
        except Exception as exc:
            return cache_pb2.PutResponse(ok=False, error=str(exc))

    async def Delete(self, request: cache_pb2.DeleteRequest, context: grpc.aio.ServicerContext) -> cache_pb2.DeleteResponse:
        try:
            ok = await storage.delete(request.key)
            return cache_pb2.DeleteResponse(ok=ok)
        except Exception as exc:
            return cache_pb2.DeleteResponse(ok=False, error=str(exc))

    async def ListEntries(
        self, request: cache_pb2.ListEntriesRequest, context: grpc.aio.ServicerContext
    ) -> cache_pb2.ListEntriesResponse:
        entries = await storage.list_entries()
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
        entry = await storage.get_entry(request.key)
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
            applied = await storage.upsert_replica(
                request.key,
                request.value,
                request.expires_at if request.expires_at > 0 else None,
                request.version,
            )
            return cache_pb2.PutReplicaResponse(ok=True, applied=applied)
        except Exception as exc:
            return cache_pb2.PutReplicaResponse(ok=False, applied=False, error=str(exc))

    async def Health(self, request: cache_pb2.HealthRequest, context: grpc.aio.ServicerContext) -> cache_pb2.HealthResponse:
        return cache_pb2.HealthResponse(node_id=NODE_ID, ok=True)


async def serve() -> None:
    await storage.initialize()
    server = grpc.aio.server()
    cache_pb2_grpc.add_CacheNodeServicer_to_server(CacheNodeService(), server)
    server.add_insecure_port(f"0.0.0.0:{GRPC_PORT}")
    await server.start()
    logger.info("node %s listening on %s", NODE_ID, GRPC_PORT)

    await _start_membership_loop()

    stop_event = asyncio.Event()

    def _handle_stop() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_stop)
        except NotImplementedError:
            continue

    await stop_event.wait()
    await _shutdown_membership_loop()
    await server.stop(grace=2)


async def _start_membership_loop() -> None:
    global heartbeat_task
    try:
        key_count = await storage.count()
        await membership.register(key_count)
    except Exception as exc:
        logger.warning("membership register failed: %s", exc)
    heartbeat_task = asyncio.create_task(_heartbeat_loop())


async def _shutdown_membership_loop() -> None:
    global heartbeat_task
    if heartbeat_task:
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat_task
        heartbeat_task = None
    try:
        await membership.deregister()
    except Exception as exc:
        logger.warning("membership deregister failed: %s", exc)
    await membership.close()
    await client_pool.close()


async def _heartbeat_loop() -> None:
    last_ring_version = -1
    while True:
        try:
            key_count = await storage.count()
            ring_version, nodes = await membership.heartbeat(key_count)
            if ring_version != last_ring_version:
                last_ring_version = ring_version
                if nodes:
                    asyncio.create_task(_rebalance(nodes))
        except Exception as exc:
            logger.warning("heartbeat failed: %s", exc)
        await asyncio.sleep(HEARTBEAT_INTERVAL)


async def _rebalance(nodes: list[str]) -> None:
    if not nodes:
        return
    async with rebalance_lock:
        start = time.perf_counter()
        keys_moved, replicas_restored = await _rebalance_once(nodes)
        duration = time.perf_counter() - start
        await membership.report_rebalance(duration, keys_moved, replicas_restored)


async def _rebalance_once(nodes: list[str]) -> tuple[int, int]:
    ring = ConsistentHashRing(nodes)
    entries = await storage.list_entries()
    keys_moved = 0
    replicas_restored = 0
    for key, value, expires_at, version in entries:
        replicas = ring.get_nodes(key, REPLICATION_FACTOR)
        if not replicas:
            continue
        applied_any = False
        for replica in replicas:
            if replica == NODE_ADDRESS:
                continue
            try:
                stub = await client_pool.get_stub(replica)
                response = await stub.PutReplica(
                    cache_pb2.PutReplicaRequest(
                        key=key,
                        value=value,
                        expires_at=expires_at or 0.0,
                        version=version,
                    ),
                    timeout=GRPC_TIMEOUT,
                )
                if response.ok and response.applied:
                    replicas_restored += 1
                    applied_any = True
            except Exception:
                continue
        if NODE_ADDRESS not in replicas and applied_any:
            removed = await storage.delete(key)
            if removed:
                keys_moved += 1
    return keys_moved, replicas_restored


if __name__ == "__main__":
    asyncio.run(serve())
