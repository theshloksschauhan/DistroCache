from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Any

import grpc
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Response, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import generate_latest
from starlette.requests import HTTPConnection

from distrocache_common.generated import cache_pb2
from distrocache_common.grpc_client import GrpcClientPool, rpc_error_text
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

# One answering replica is enough. Stage 4 replaces this with R and W.
_REQUIRED_ACKS = 1

# gRPC's default max reconnect backoff is 120s, so a channel that watched a node die
# can ignore the restart for two minutes. 100ms floor, 200ms first wait, 2s cap:
# a short blip reconnects inside one REQUEST_TIMEOUT (1s), a dead node is probed at
# most 10 times a second and then once every 2s, and 2s still fits in a few seconds
# on top of the sequential fan-out's per-attempt deadline.
_RECONNECT_BACKOFF: tuple[tuple[str, int], ...] = (
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.initial_reconnect_backoff_ms", 200),
    ("grpc.max_reconnect_backoff_ms", 2000),
)


def _retryable(exc: BaseException) -> bool:
    """True only for statuses that mean 'the call may work if sent again'."""
    code = getattr(exc, "code", None)
    if not callable(code):
        return False
    try:
        status = code()
    except Exception:
        return False
    return status in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)


def _failed_vote(operation: str, node: str, error: str, errors: list[dict[str, str]]) -> None:
    errors.append({"node": node, "error": error})
    logger.warning("%s on %s failed: %s", operation, node, error)


def _unavailable(operation: str, acks: int, errors: list[dict[str, str]]) -> JSONResponse:
    REQUEST_FAILURES.labels(operation=operation, reason="unavailable").inc()
    return JSONResponse(
        status_code=503,
        content={"acks": acks, "required": _REQUIRED_ACKS, "errors": errors},
    )


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
        self.clients = GrpcClientPool(channel_options=_RECONNECT_BACKOFF)
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

    async def _rpc(self, node: str, method: str, request: Any) -> Any:
        """One replica call. Retry only when another try could succeed.

        UNAVAILABLE and DEADLINE_EXCEEDED are retried for reads and writes. A timed-out
        write may already be applied; the retry sends it again. Stage 3 makes that safe
        by reusing one version and treating ALREADY_CURRENT as an ack.
        """
        stub = await self.clients.get_stub(node)
        for attempt in range(self.settings.retry_attempts):
            try:
                return await getattr(stub, method)(request, timeout=self.settings.request_timeout)
            except Exception as exc:
                retrying = attempt + 1 < self.settings.retry_attempts and _retryable(exc)
                if not retrying:
                    raise
                await asyncio.sleep(self.settings.retry_backoff * (2**attempt))
        raise RuntimeError("gRPC call failed")

    async def grpc_get(self, node: str, key: str) -> cache_pb2.GetResponse:
        return await self._rpc(node, "Get", cache_pb2.GetRequest(key=key))

    async def grpc_put(self, node: str, key: str, value: bytes) -> cache_pb2.PutResponse:
        return await self._rpc(node, "Put", cache_pb2.PutRequest(key=key, value=value))

    async def grpc_delete(self, node: str, key: str) -> cache_pb2.DeleteResponse:
        return await self._rpc(node, "Delete", cache_pb2.DeleteRequest(key=key))

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


def _no_nodes(operation: str) -> JSONResponse:
    return _unavailable(operation, acks=0, errors=[])


@router.get("/kv/{key}", response_model=None)
async def get_key(key: str, c: CoordinatorDep) -> dict[str, Any] | JSONResponse:
    REQUEST_COUNT.labels(operation="get").inc()
    nodes = await c.replicas_for(key)
    if not nodes:
        return _no_nodes("get")

    acks = 0
    errors: list[dict[str, str]] = []
    with LATENCY.labels(operation="get").time():
        for node in nodes:
            try:
                response = await c.grpc_get(node, key)
            except Exception as exc:
                _failed_vote("get", node, rpc_error_text(exc), errors)
                continue
            acks += 1
            if not response.found:
                continue
            try:
                value = response.value.decode("utf-8")
            except UnicodeDecodeError:
                REQUEST_FAILURES.labels(operation="get", reason="internal").inc()
                raise HTTPException(
                    status_code=500, detail="Stored value is not valid UTF-8"
                ) from None
            HIT_COUNT.labels(operation="get").inc()
            return {"key": key, "value": value, "node": node}

    # A miss is a successful answer. Replicas that failed are not misses: with required=1
    # a single answering replica that lacks the key decides the read, even when the
    # replicas that hold it failed. Stage 4's read quorum fixes that.
    if acks >= _REQUIRED_ACKS:
        raise HTTPException(status_code=404, detail="Key not found")
    return _unavailable("get", acks, errors)


@router.post("/kv/{key}", response_model=None)
async def put_key(
    key: str, payload: dict[str, str], c: CoordinatorDep
) -> dict[str, Any] | JSONResponse:
    REQUEST_COUNT.labels(operation="put").inc()
    value = payload.get("value")
    if value is None:
        raise HTTPException(status_code=400, detail="Missing value")
    nodes = await c.replicas_for(key)
    if not nodes:
        return _no_nodes("put")

    with LATENCY.labels(operation="put").time():
        acks, errors = await _write_votes(
            "put", nodes, lambda node: c.grpc_put(node, key, value.encode())
        )
    if acks < _REQUIRED_ACKS:
        return _unavailable("put", acks, errors)
    return {"key": key, "replicas": nodes, "failures": len(errors)}


@router.delete("/kv/{key}", response_model=None)
async def delete_key(key: str, c: CoordinatorDep) -> dict[str, Any] | JSONResponse:
    REQUEST_COUNT.labels(operation="delete").inc()
    nodes = await c.replicas_for(key)
    if not nodes:
        return _no_nodes("delete")

    with LATENCY.labels(operation="delete").time():
        acks, errors = await _write_votes("delete", nodes, lambda node: c.grpc_delete(node, key))
    if acks < _REQUIRED_ACKS:
        return _unavailable("delete", acks, errors)
    return {"key": key, "replicas": nodes, "failures": len(errors)}


async def _write_votes(
    operation: str,
    nodes: list[str],
    call: Callable[[str], Awaitable[Any]],
) -> tuple[int, list[dict[str, str]]]:
    acks = 0
    errors: list[dict[str, str]] = []
    for node in nodes:
        try:
            response = await call(node)
        except Exception as exc:
            _failed_vote(operation, node, rpc_error_text(exc), errors)
            continue
        if response.ok:
            acks += 1
        else:
            _failed_vote(operation, node, response.error or "not ok", errors)
    return acks, errors


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
