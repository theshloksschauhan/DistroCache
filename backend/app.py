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
from distrocache_common.hlc import HLC, HybridClock

from .membership import MembershipRegistry
from .metrics import (
    ACTIVE_NODES,
    HEARTBEAT_FAILURES,
    HIT_COUNT,
    LATENCY,
    MISS_COUNT,
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

# A stored write version as a tuple, compared directly: (physical_ms, logical, coordinator_id).
NodeVersion = tuple[int, int, str]

# A versioned write acks if the node stored it (APPLIED) or already held it or a newer copy
# (ALREADY_CURRENT); the latter is what makes a retried write idempotent.
_ACK_STATUSES = (cache_pb2.WriteStatus.APPLIED, cache_pb2.WriteStatus.ALREADY_CURRENT)


def _version_proto(version: HLC) -> cache_pb2.Version:
    return cache_pb2.Version(
        physical_ms=version.physical_ms,
        logical=version.logical,
        coordinator_id=version.coordinator_id,
    )


def _version_dict(version: HLC | NodeVersion) -> dict[str, Any]:
    if isinstance(version, HLC):
        return {
            "physical_ms": version.physical_ms,
            "logical": version.logical,
            "coordinator_id": version.coordinator_id,
        }
    return {"physical_ms": version[0], "logical": version[1], "coordinator_id": version[2]}


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


def _unavailable(
    operation: str, acks: int, required: int, errors: list[dict[str, str]]
) -> JSONResponse:
    REQUEST_FAILURES.labels(operation=operation, reason="unavailable").inc()
    return JSONResponse(
        status_code=503,
        content={"acks": acks, "required": required, "errors": errors},
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
    read_quorum: int = 2
    write_quorum: int = 2
    ttl_seconds: int = 0
    coordinator_id: str = "coordinator"
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
        read_quorum=int(os.getenv("READ_QUORUM", "2")),
        write_quorum=int(os.getenv("WRITE_QUORUM", "2")),
        ttl_seconds=int(os.getenv("TTL_SECONDS", "0")),
        coordinator_id=os.getenv("COORDINATOR_ID", "coordinator"),
        cors_origins=cors_origins,
    )


def _validate_quorums(settings: Settings) -> None:
    n, r, w = settings.replication_factor, settings.read_quorum, settings.write_quorum
    if not (1 <= r <= n and 1 <= w <= n):
        raise ValueError(f"need 1 <= R <= N and 1 <= W <= N; got N={n}, R={r}, W={w}")
    if r + w > n:
        logger.info("R + W > N (%d + %d > %d): a read sees the latest acked write", r, w, n)
    else:
        logger.warning("R + W <= N (%d + %d <= %d): a read may miss the latest write", r, w, n)


class Coordinator:
    """Per-app state: membership, gRPC clients, dashboard state and background loops.

    One instance per FastAPI app (not module globals), so each test can build a fresh one.
    """

    def __init__(self, settings: Settings) -> None:
        _validate_quorums(settings)
        self.settings = settings
        self.clock = HybridClock(settings.coordinator_id)
        self.clients = GrpcClientPool(channel_options=_RECONNECT_BACKOFF)
        self.ws_manager = WebSocketManager()
        self.registry = MembershipRegistry(
            timeout_seconds=settings.heartbeat_timeout, seed_nodes=settings.node_addresses
        )
        self.recent_rebalances: list[dict[str, Any]] = []
        self.cluster_state: dict[str, dict[str, Any]] = {}
        self._tasks: list[asyncio.Task[None]] = []
        # Writes that have met quorum but whose remaining replicas are still finishing. Held so
        # the event loop does not garbage-collect them before they complete.
        self._background: set[asyncio.Task[Any]] = set()

    def start(self) -> None:
        self._spawn(self._refresh_health_loop())
        self._spawn(self._prune_membership_loop())

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        self._tasks.append(asyncio.create_task(coro))

    def detach(self, tasks: set[asyncio.Task[Any]]) -> None:
        """Let already-running tasks finish without blocking the response."""
        for task in tasks:
            self._background.add(task)
            task.add_done_callback(self._background.discard)

    async def stop(self) -> None:
        for task in [*self._tasks, *self._background]:
            task.cancel()
        await asyncio.gather(*self._tasks, *self._background, return_exceptions=True)
        await self.clients.close()

    async def _refresh_health_loop(self) -> None:
        while True:
            try:
                await self._refresh_health_once()
            except Exception:
                # One bad poll must not end the loop: harness readiness and the
                # dashboard both depend on it. CancelledError is a BaseException,
                # so shutdown still cancels the task.
                logger.exception("health loop iteration failed")
            await asyncio.sleep(self.settings.health_interval)

    async def _refresh_health_once(self) -> None:
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

    async def _prune_membership_loop(self) -> None:
        while True:
            try:
                await self._prune_membership_once()
            except Exception:
                logger.exception("membership prune iteration failed")
            await asyncio.sleep(self.settings.heartbeat_interval)

    async def _prune_membership_once(self) -> None:
        nodes = await self.registry.prune_expired()
        ACTIVE_NODES.set(len([node for node in nodes if node.status == "up"]))

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

    async def grpc_put(
        self, node: str, key: str, value: bytes, version: HLC, expires_at: float
    ) -> cache_pb2.PutResponse:
        request = cache_pb2.PutRequest(
            key=key, value=value, version=_version_proto(version), expires_at=expires_at
        )
        return await self._rpc(node, "Put", request)

    async def grpc_delete(
        self, node: str, key: str, version: HLC, expires_at: float
    ) -> cache_pb2.DeleteResponse:
        request = cache_pb2.DeleteRequest(
            key=key, version=_version_proto(version), expires_at=expires_at
        )
        return await self._rpc(node, "Delete", request)

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


def _no_nodes(operation: str, required: int) -> JSONResponse:
    return _unavailable(operation, acks=0, required=required, errors=[])


# One replica's reply to a read: ("found", node, None, version, value) with a value,
# ("miss", node, None, None, None) for a key the replica does not hold, or
# ("error", node, text, None, None) for a failed RPC. It never raises, so a detached
# vote left running past the quorum cannot produce an unretrieved exception.
ReadVote = tuple[str, str, str | None, NodeVersion | None, bytes | None]


async def _read_vote(c: Coordinator, node: str, key: str) -> ReadVote:
    try:
        response = await c.grpc_get(node, key)
    except Exception as exc:
        return ("error", node, rpc_error_text(exc), None, None)
    if response.found:
        v = response.version
        return ("found", node, None, (v.physical_ms, v.logical, v.coordinator_id), response.value)
    return ("miss", node, None, None, None)


async def _read_quorum(
    c: Coordinator, key: str, nodes: list[str], threshold: int
) -> tuple[int, list[dict[str, str]], tuple[NodeVersion, bytes, str] | None]:
    """Query all replicas in parallel; stop once `threshold` have responded.

    A response is a value or a miss; a failed RPC is not. Returns the number of responses, the
    failed-vote errors, and the highest-versioned value seen (or None if every response missed).
    """
    pending = {asyncio.create_task(_read_vote(c, node, key)) for node in nodes}
    responses = 0
    errors: list[dict[str, str]] = []
    best: tuple[NodeVersion, bytes, str] | None = None
    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            kind, node, error, version, value = task.result()
            if kind == "error":
                errors.append({"node": node, "error": error or "read error"})
                logger.warning("get on %s failed: %s", node, error)
                continue
            responses += 1
            if kind == "found" and version is not None and value is not None:
                if best is None or version > best[0]:
                    best = (version, value, node)
        if responses >= threshold:
            break
    c.detach(pending)  # late responses are ignored this stage; Stage 6 repairs from them
    return responses, errors, best


@router.get("/kv/{key}", response_model=None)
async def get_key(key: str, c: CoordinatorDep) -> dict[str, Any] | JSONResponse:
    REQUEST_COUNT.labels(operation="get").inc()
    nodes = await c.replicas_for(key)
    if not nodes:
        return _no_nodes("get", c.settings.read_quorum)

    threshold = min(c.settings.read_quorum, len(nodes))
    with LATENCY.labels(operation="get").time():
        responses, errors, best = await _read_quorum(c, key, nodes, threshold)

    if responses < threshold:
        # Too few replicas answered; the rest failed. Not a miss.
        return _unavailable("get", responses, c.settings.read_quorum, errors)
    if best is None:
        MISS_COUNT.labels(operation="get").inc()
        raise HTTPException(status_code=404, detail="Key not found")
    version, value_bytes, node = best
    try:
        value = value_bytes.decode("utf-8")
    except UnicodeDecodeError:
        REQUEST_FAILURES.labels(operation="get", reason="internal").inc()
        raise HTTPException(status_code=500, detail="Stored value is not valid UTF-8") from None
    HIT_COUNT.labels(operation="get").inc()
    return {
        "key": key,
        "value": value,
        "node": node,
        "acks": responses,
        "version": _version_dict(version),
    }


def _expires_at(ttl_seconds: int) -> float:
    # Absolute wall-clock expiry so every replica of a key expires together. 0 means no expiry.
    return time.time() + ttl_seconds if ttl_seconds > 0 else 0.0


def _ttl_from(payload: dict[str, Any], default: int) -> int:
    raw = payload.get("ttl_seconds")
    return default if raw is None else int(raw)


@router.post("/kv/{key}", response_model=None)
async def put_key(
    key: str, payload: dict[str, Any], c: CoordinatorDep
) -> dict[str, Any] | JSONResponse:
    REQUEST_COUNT.labels(operation="put").inc()
    value = payload.get("value")
    if value is None:
        raise HTTPException(status_code=400, detail="Missing value")
    nodes = await c.replicas_for(key)
    if not nodes:
        return _no_nodes("put", c.settings.write_quorum)

    # One version and one expiry for every replica and every retry, assigned here.
    version = c.clock.tick()
    expires_at = _expires_at(_ttl_from(payload, c.settings.ttl_seconds))
    threshold = min(c.settings.write_quorum, len(nodes))
    with LATENCY.labels(operation="put").time():
        acks, errors = await _write_quorum(
            c,
            "put",
            nodes,
            lambda node: c.grpc_put(node, key, value.encode(), version, expires_at),
            threshold,
        )
    if acks < threshold:
        return _unavailable("put", acks, c.settings.write_quorum, errors)
    return {"key": key, "replicas": nodes, "acks": acks, "version": _version_dict(version)}


@router.delete("/kv/{key}", response_model=None)
async def delete_key(key: str, c: CoordinatorDep) -> dict[str, Any] | JSONResponse:
    REQUEST_COUNT.labels(operation="delete").inc()
    nodes = await c.replicas_for(key)
    if not nodes:
        return _no_nodes("delete", c.settings.write_quorum)

    version = c.clock.tick()
    threshold = min(c.settings.write_quorum, len(nodes))
    with LATENCY.labels(operation="delete").time():
        acks, errors = await _write_quorum(
            c, "delete", nodes, lambda node: c.grpc_delete(node, key, version, 0.0), threshold
        )
    if acks < threshold:
        return _unavailable("delete", acks, c.settings.write_quorum, errors)
    return {"key": key, "replicas": nodes, "acks": acks, "version": _version_dict(version)}


async def _write_vote(
    operation: str, node: str, call: Callable[[str], Awaitable[Any]]
) -> tuple[bool, dict[str, str] | None]:
    """One replica's write vote; never raises so a detached vote finishes cleanly."""
    try:
        response = await call(node)
    except Exception as exc:
        text = rpc_error_text(exc)
        logger.warning("%s on %s failed: %s", operation, node, text)
        return False, {"node": node, "error": text}
    if response.status in _ACK_STATUSES:
        return True, None
    text = response.error or "write error"
    logger.warning("%s on %s failed: %s", operation, node, text)
    return False, {"node": node, "error": text}


async def _write_quorum(
    c: Coordinator,
    operation: str,
    nodes: list[str],
    call: Callable[[str], Awaitable[Any]],
    threshold: int,
) -> tuple[int, list[dict[str, str]]]:
    """Fan a write out to all replicas; return once `threshold` ack, leaving the rest running."""
    pending = {asyncio.create_task(_write_vote(operation, node, call)) for node in nodes}
    acks = 0
    errors: list[dict[str, str]] = []
    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            ok, error = task.result()
            if ok:
                acks += 1
            elif error is not None:
                errors.append(error)
        if acks >= threshold:
            break
    c.detach(pending)  # a briefly-slow replica still receives the write in the background
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
