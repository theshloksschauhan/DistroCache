from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import AsyncIterator, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Any

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Response, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest
from starlette.requests import HTTPConnection

from distrocache_common.generated import cache_pb2
from distrocache_common.grpc_client import GrpcClientPool
from distrocache_common.hashing import ConsistentHashRing

from .membership import MembershipRegistry
from .metrics import (
    ACTIVE_NODES,
    HEARTBEAT_FAILURES,
    HIT_COUNT,
    LATENCY,
    NODE_DEREGISTRATIONS,
    NODE_REGISTRATIONS,
    NODE_STATUS,
    REBALANCE_DURATION,
    REBALANCE_EVENTS,
    REPLICA_RESTORES,
    REQUEST_COUNT,
    REQUEST_FAILURES,
)
from .websocket_manager import WebSocketManager

logger = logging.getLogger("distrocache.api")


@dataclass(frozen=True)
class Settings:
    node_addresses: list[str]
    replication_factor: int = 3
    health_interval: float = 2.0
    heartbeat_interval: float = 2.0
    heartbeat_timeout: float = 6.0
    request_timeout: float = 1.0
    retry_attempts: int = 2
    retry_backoff: float = 0.1
    cors_origins: tuple[str, ...] = ("http://localhost:3000", "http://localhost:5173")


def load_settings() -> Settings:
    raw_addresses = os.getenv("NODE_ADDRESSES", "")
    cors_raw = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:5173")
    cors_origins = (
        ("*",)
        if cors_raw.strip() == "*"
        else tuple(origin.strip() for origin in cors_raw.split(",") if origin.strip())
    )
    return Settings(
        node_addresses=raw_addresses.split(",") if raw_addresses else [],
        replication_factor=int(os.getenv("REPLICATION_FACTOR", "3")),
        health_interval=float(os.getenv("HEALTH_INTERVAL", "2.0")),
        heartbeat_interval=float(os.getenv("HEARTBEAT_INTERVAL", "2.0")),
        heartbeat_timeout=float(os.getenv("HEARTBEAT_TIMEOUT", "6.0")),
        request_timeout=float(os.getenv("REQUEST_TIMEOUT", "1.0")),
        retry_attempts=int(os.getenv("RETRY_ATTEMPTS", "2")),
        retry_backoff=float(os.getenv("RETRY_BACKOFF", "0.1")),
        cors_origins=cors_origins,
    )


class Coordinator:
    """Per-app state: membership, gRPC clients, dashboard state and background loops.

    One instance per FastAPI app (not module globals), so each test can build a fresh one.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.clients = GrpcClientPool()
        self.ws_manager = WebSocketManager()
        self.registry = MembershipRegistry(
            timeout_seconds=settings.heartbeat_timeout, seed_nodes=settings.node_addresses
        )
        self.recent_rebalances: list[dict[str, Any]] = []
        self.cluster_state: dict[str, dict[str, Any]] = {}
        self._tasks: list[asyncio.Task[None]] = []

    def start(self) -> None:
        self._spawn(self._refresh_health_loop())
        self._spawn(self._prune_membership_loop())

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        self._tasks.append(asyncio.create_task(coro))

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.clients.close()

    async def _refresh_health_loop(self) -> None:
        while True:
            nodes = await self.registry.live_addresses()
            for node in nodes:
                start = time.perf_counter()
                ok = False
                try:
                    stub = await self.clients.get_stub(node)
                    response = await stub.Health(
                        cache_pb2.HealthRequest(), timeout=self.settings.request_timeout
                    )
                    ok = response.ok
                except Exception as exc:
                    logger.warning("health check failed for %s: %s", node, exc)
                    ok = False
                latency_ms = (time.perf_counter() - start) * 1000
                self.cluster_state[node] = {
                    "ok": ok,
                    "last_seen": time.time(),
                    "latency_ms": latency_ms,
                }
                NODE_STATUS.labels(node=node).set(1 if ok else 0)
            ACTIVE_NODES.set(len(nodes))
            await self.ws_manager.broadcast(
                {
                    "type": "cluster",
                    "nodes": self.cluster_state,
                    "rebalances": self.recent_rebalances[-10:],
                }
            )
            await asyncio.sleep(self.settings.health_interval)

    async def _prune_membership_loop(self) -> None:
        while True:
            nodes = await self.registry.prune_expired()
            ACTIVE_NODES.set(len([node for node in nodes if node.status == "up"]))
            await asyncio.sleep(self.settings.heartbeat_interval)

    async def grpc_get(self, node: str, key: str) -> cache_pb2.GetResponse:
        stub = await self.clients.get_stub(node)
        last_exc: Exception | None = None
        for attempt in range(self.settings.retry_attempts):
            try:
                return await stub.Get(
                    cache_pb2.GetRequest(key=key), timeout=self.settings.request_timeout
                )
            except Exception as exc:
                last_exc = exc
                if attempt < self.settings.retry_attempts - 1:
                    await asyncio.sleep(self.settings.retry_backoff * (2**attempt))
        raise last_exc or RuntimeError("gRPC get failed")

    async def grpc_put(self, node: str, key: str, value: bytes) -> cache_pb2.PutResponse:
        stub = await self.clients.get_stub(node)
        last_exc: Exception | None = None
        for attempt in range(self.settings.retry_attempts):
            try:
                return await stub.Put(
                    cache_pb2.PutRequest(key=key, value=value),
                    timeout=self.settings.request_timeout,
                )
            except Exception as exc:
                last_exc = exc
                if attempt < self.settings.retry_attempts - 1:
                    await asyncio.sleep(self.settings.retry_backoff * (2**attempt))
        raise last_exc or RuntimeError("gRPC put failed")

    async def grpc_delete(self, node: str, key: str) -> cache_pb2.DeleteResponse:
        stub = await self.clients.get_stub(node)
        last_exc: Exception | None = None
        for attempt in range(self.settings.retry_attempts):
            try:
                return await stub.Delete(
                    cache_pb2.DeleteRequest(key=key), timeout=self.settings.request_timeout
                )
            except Exception as exc:
                last_exc = exc
                if attempt < self.settings.retry_attempts - 1:
                    await asyncio.sleep(self.settings.retry_backoff * (2**attempt))
        raise last_exc or RuntimeError("gRPC delete failed")

    async def replicas_for(self, key: str) -> list[str]:
        nodes = await self.registry.live_addresses()
        return ConsistentHashRing(nodes).get_nodes(key, self.settings.replication_factor)


def get_coordinator(connection: HTTPConnection) -> Coordinator:
    coordinator: Coordinator = connection.app.state.coordinator
    return coordinator


CoordinatorDep = Annotated[Coordinator, Depends(get_coordinator)]

router = APIRouter()


@router.get("/health")
async def health(c: CoordinatorDep) -> dict[str, Any]:
    return {"nodes": c.cluster_state}


@router.get("/membership/nodes")
async def membership_nodes(c: CoordinatorDep) -> dict[str, Any]:
    nodes = await c.registry.snapshot()
    return {
        "nodes": [
            {
                "node_id": node.node_id,
                "address": node.address,
                "status": node.status,
                "last_seen": node.last_seen,
                "key_count": node.key_count,
            }
            for node in nodes
        ],
        "ring_version": await c.registry.ring_version(),
    }


@router.post("/membership/register")
async def membership_register(payload: dict[str, Any], c: CoordinatorDep) -> dict[str, Any]:
    node_id = payload.get("node_id")
    address = payload.get("address")
    key_count = int(payload.get("key_count", 0))
    if not node_id or not address:
        raise HTTPException(status_code=400, detail="Missing node_id or address")
    ring_version = await c.registry.register(node_id=node_id, address=address, key_count=key_count)
    NODE_REGISTRATIONS.inc()
    return {
        "ring_version": ring_version,
        "nodes": await c.registry.live_addresses(),
    }


@router.post("/membership/heartbeat")
async def membership_heartbeat(payload: dict[str, Any], c: CoordinatorDep) -> dict[str, Any]:
    node_id = payload.get("node_id")
    address = payload.get("address")
    key_count = int(payload.get("key_count", 0))
    if not node_id or not address:
        HEARTBEAT_FAILURES.inc()
        raise HTTPException(status_code=400, detail="Missing node_id or address")
    ring_version = await c.registry.heartbeat(node_id=node_id, address=address, key_count=key_count)
    return {
        "ring_version": ring_version,
        "nodes": await c.registry.live_addresses(),
    }


@router.post("/membership/deregister")
async def membership_deregister(payload: dict[str, Any], c: CoordinatorDep) -> dict[str, Any]:
    node_id = payload.get("node_id")
    if not node_id:
        raise HTTPException(status_code=400, detail="Missing node_id")
    ring_version = await c.registry.deregister(node_id=node_id)
    NODE_DEREGISTRATIONS.inc()
    return {"ring_version": ring_version}


@router.post("/membership/rebalance")
async def membership_rebalance(payload: dict[str, Any], c: CoordinatorDep) -> dict[str, Any]:
    node_id = payload.get("node_id")
    duration_s = float(payload.get("duration_s", 0.0))
    keys_moved = int(payload.get("keys_moved", 0))
    replicas_restored = int(payload.get("replicas_restored", 0))
    if not node_id:
        raise HTTPException(status_code=400, detail="Missing node_id")
    REBALANCE_EVENTS.inc()
    REBALANCE_DURATION.observe(duration_s)
    if replicas_restored:
        REPLICA_RESTORES.inc(replicas_restored)
    c.recent_rebalances.append(
        {
            "node_id": node_id,
            "duration_s": duration_s,
            "keys_moved": keys_moved,
            "replicas_restored": replicas_restored,
            "timestamp": time.time(),
        }
    )
    return {"ok": True}


@router.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type="text/plain")


@router.websocket("/ws/cluster")
async def cluster_ws(websocket: WebSocket, c: CoordinatorDep) -> None:
    await c.ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except Exception:
        c.ws_manager.disconnect(websocket)


@router.get("/kv/{key}")
async def get_key(key: str, c: CoordinatorDep) -> dict[str, Any]:
    REQUEST_COUNT.labels(operation="get").inc()
    nodes = await c.replicas_for(key)
    if not nodes:
        raise HTTPException(status_code=503, detail="No cache nodes available")

    with LATENCY.labels(operation="get").time():
        for node in nodes:
            response = await c.grpc_get(node, key)
            if response.found:
                HIT_COUNT.labels(operation="get").inc()
                return {"key": key, "value": response.value.decode("utf-8"), "node": node}

    REQUEST_FAILURES.labels(operation="get").inc()
    raise HTTPException(status_code=404, detail="Key not found")


@router.post("/kv/{key}")
async def put_key(key: str, payload: dict[str, str], c: CoordinatorDep) -> dict[str, Any]:
    REQUEST_COUNT.labels(operation="put").inc()
    value = payload.get("value")
    if value is None:
        raise HTTPException(status_code=400, detail="Missing value")
    nodes = await c.replicas_for(key)
    if not nodes:
        raise HTTPException(status_code=503, detail="No cache nodes available")

    failures = 0
    with LATENCY.labels(operation="put").time():
        for node in nodes:
            try:
                response = await c.grpc_put(node, key, value.encode("utf-8"))
                if not response.ok:
                    failures += 1
            except Exception:
                failures += 1

    if failures == len(nodes):
        REQUEST_FAILURES.labels(operation="put").inc()
        raise HTTPException(status_code=503, detail="Write failed on all replicas")
    return {"key": key, "replicas": nodes, "failures": failures}


@router.delete("/kv/{key}")
async def delete_key(key: str, c: CoordinatorDep) -> dict[str, Any]:
    REQUEST_COUNT.labels(operation="delete").inc()
    nodes = await c.replicas_for(key)
    if not nodes:
        raise HTTPException(status_code=503, detail="No cache nodes available")

    failures = 0
    with LATENCY.labels(operation="delete").time():
        for node in nodes:
            try:
                response = await c.grpc_delete(node, key)
                if not response.ok:
                    failures += 1
            except Exception:
                failures += 1

    if failures == len(nodes):
        REQUEST_FAILURES.labels(operation="delete").inc()
        raise HTTPException(status_code=503, detail="Delete failed on all replicas")
    return {"key": key, "replicas": nodes, "failures": failures}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    coordinator: Coordinator = app.state.coordinator
    coordinator.start()
    try:
        yield
    finally:
        await coordinator.stop()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    app = FastAPI(title="DistroCache API", lifespan=lifespan)
    # Building the coordinator opens nothing: gRPC channels are created on first use
    # and the background loops only start in lifespan.
    app.state.coordinator = Coordinator(settings)
    cors_origins = list(settings.cors_origins)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=cors_origins != ["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(router)
    return app


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
# Module-level app so `uvicorn backend.app:app` keeps working.
app = create_app()
