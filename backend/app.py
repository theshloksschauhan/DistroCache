from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Response, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest

from common.generated import cache_pb2
from common.grpc_client import GrpcClientPool
from common.hashing import ConsistentHashRing

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

NODE_ADDRESSES = os.getenv("NODE_ADDRESSES", "").split(",") if os.getenv("NODE_ADDRESSES") else []
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "3"))
HEALTH_INTERVAL = float(os.getenv("HEALTH_INTERVAL", "2.0"))
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "2.0"))
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "6.0"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "1.0"))
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "2"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF", "0.1"))
CORS_ORIGINS_RAW = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:3000,http://localhost:5173",
)
CORS_ORIGINS = (
    ["*"]
    if CORS_ORIGINS_RAW.strip() == "*"
    else [origin.strip() for origin in CORS_ORIGINS_RAW.split(",") if origin.strip()]
)

clients = GrpcClientPool()
ws_manager = WebSocketManager()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("distrocache.api")
registry = MembershipRegistry(timeout_seconds=HEARTBEAT_TIMEOUT, seed_nodes=NODE_ADDRESSES)

recent_rebalances: list[dict[str, Any]] = []

cluster_state: dict[str, dict[str, Any]] = {}


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    asyncio.create_task(refresh_health())
    asyncio.create_task(prune_membership())
    yield
    await clients.close()


app = FastAPI(title="DistroCache API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False if CORS_ORIGINS == ["*"] else True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def refresh_health() -> None:
    while True:
        nodes = await registry.live_addresses()
        for node in nodes:
            start = time.perf_counter()
            ok = False
            try:
                stub = await clients.get_stub(node)
                response = await stub.Health(cache_pb2.HealthRequest(), timeout=REQUEST_TIMEOUT)
                ok = response.ok
            except Exception as exc:
                logger.warning("health check failed for %s: %s", node, exc)
                ok = False
            latency_ms = (time.perf_counter() - start) * 1000
            cluster_state[node] = {
                "ok": ok,
                "last_seen": time.time(),
                "latency_ms": latency_ms,
            }
            NODE_STATUS.labels(node=node).set(1 if ok else 0)
        ACTIVE_NODES.set(len(nodes))
        await ws_manager.broadcast(
            {"type": "cluster", "nodes": cluster_state, "rebalances": recent_rebalances[-10:]}
        )
        await asyncio.sleep(HEALTH_INTERVAL)


async def prune_membership() -> None:
    while True:
        nodes = await registry.prune_expired()
        ACTIVE_NODES.set(len([node for node in nodes if node.status == "up"]))
        await asyncio.sleep(HEARTBEAT_INTERVAL)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"nodes": cluster_state}


@app.get("/membership/nodes")
async def membership_nodes() -> dict[str, Any]:
    nodes = await registry.snapshot()
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
        "ring_version": await registry.ring_version(),
    }


@app.post("/membership/register")
async def membership_register(payload: dict[str, Any]) -> dict[str, Any]:
    node_id = payload.get("node_id")
    address = payload.get("address")
    key_count = int(payload.get("key_count", 0))
    if not node_id or not address:
        raise HTTPException(status_code=400, detail="Missing node_id or address")
    ring_version = await registry.register(node_id=node_id, address=address, key_count=key_count)
    NODE_REGISTRATIONS.inc()
    return {
        "ring_version": ring_version,
        "nodes": await registry.live_addresses(),
    }


@app.post("/membership/heartbeat")
async def membership_heartbeat(payload: dict[str, Any]) -> dict[str, Any]:
    node_id = payload.get("node_id")
    address = payload.get("address")
    key_count = int(payload.get("key_count", 0))
    if not node_id or not address:
        HEARTBEAT_FAILURES.inc()
        raise HTTPException(status_code=400, detail="Missing node_id or address")
    ring_version = await registry.heartbeat(node_id=node_id, address=address, key_count=key_count)
    return {
        "ring_version": ring_version,
        "nodes": await registry.live_addresses(),
    }


@app.post("/membership/deregister")
async def membership_deregister(payload: dict[str, Any]) -> dict[str, Any]:
    node_id = payload.get("node_id")
    if not node_id:
        raise HTTPException(status_code=400, detail="Missing node_id")
    ring_version = await registry.deregister(node_id=node_id)
    NODE_DEREGISTRATIONS.inc()
    return {"ring_version": ring_version}


@app.post("/membership/rebalance")
async def membership_rebalance(payload: dict[str, Any]) -> dict[str, Any]:
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
    recent_rebalances.append(
        {
            "node_id": node_id,
            "duration_s": duration_s,
            "keys_moved": keys_moved,
            "replicas_restored": replicas_restored,
            "timestamp": time.time(),
        }
    )
    return {"ok": True}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type="text/plain")


@app.websocket("/ws/cluster")
async def cluster_ws(websocket: WebSocket) -> None:
    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except Exception:
        ws_manager.disconnect(websocket)


async def _grpc_get(node: str, key: str) -> cache_pb2.GetResponse:
    stub = await clients.get_stub(node)
    last_exc: Exception | None = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            return await stub.Get(cache_pb2.GetRequest(key=key), timeout=REQUEST_TIMEOUT)
        except Exception as exc:
            last_exc = exc
            if attempt < RETRY_ATTEMPTS - 1:
                await asyncio.sleep(RETRY_BACKOFF * (2**attempt))
    raise last_exc or RuntimeError("gRPC get failed")


async def _grpc_put(node: str, key: str, value: bytes) -> cache_pb2.PutResponse:
    stub = await clients.get_stub(node)
    last_exc: Exception | None = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            return await stub.Put(
                cache_pb2.PutRequest(key=key, value=value), timeout=REQUEST_TIMEOUT
            )
        except Exception as exc:
            last_exc = exc
            if attempt < RETRY_ATTEMPTS - 1:
                await asyncio.sleep(RETRY_BACKOFF * (2**attempt))
    raise last_exc or RuntimeError("gRPC put failed")


async def _grpc_delete(node: str, key: str) -> cache_pb2.DeleteResponse:
    stub = await clients.get_stub(node)
    last_exc: Exception | None = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            return await stub.Delete(cache_pb2.DeleteRequest(key=key), timeout=REQUEST_TIMEOUT)
        except Exception as exc:
            last_exc = exc
            if attempt < RETRY_ATTEMPTS - 1:
                await asyncio.sleep(RETRY_BACKOFF * (2**attempt))
    raise last_exc or RuntimeError("gRPC delete failed")


@app.get("/kv/{key}")
async def get_key(key: str) -> dict[str, Any]:
    REQUEST_COUNT.labels(operation="get").inc()
    nodes = await registry.live_addresses()
    ring = ConsistentHashRing(nodes)
    nodes = ring.get_nodes(key, REPLICATION_FACTOR)
    if not nodes:
        raise HTTPException(status_code=503, detail="No cache nodes available")

    with LATENCY.labels(operation="get").time():
        for node in nodes:
            response = await _grpc_get(node, key)
            if response.found:
                HIT_COUNT.labels(operation="get").inc()
                return {"key": key, "value": response.value.decode("utf-8"), "node": node}

    REQUEST_FAILURES.labels(operation="get").inc()
    raise HTTPException(status_code=404, detail="Key not found")


@app.post("/kv/{key}")
async def put_key(key: str, payload: dict[str, str]) -> dict[str, Any]:
    REQUEST_COUNT.labels(operation="put").inc()
    value = payload.get("value")
    if value is None:
        raise HTTPException(status_code=400, detail="Missing value")
    nodes = await registry.live_addresses()
    ring = ConsistentHashRing(nodes)
    nodes = ring.get_nodes(key, REPLICATION_FACTOR)
    if not nodes:
        raise HTTPException(status_code=503, detail="No cache nodes available")

    failures = 0
    with LATENCY.labels(operation="put").time():
        for node in nodes:
            try:
                response = await _grpc_put(node, key, value.encode("utf-8"))
                if not response.ok:
                    failures += 1
            except Exception:
                failures += 1

    if failures == len(nodes):
        REQUEST_FAILURES.labels(operation="put").inc()
        raise HTTPException(status_code=503, detail="Write failed on all replicas")
    return {"key": key, "replicas": nodes, "failures": failures}


@app.delete("/kv/{key}")
async def delete_key(key: str) -> dict[str, Any]:
    REQUEST_COUNT.labels(operation="delete").inc()
    nodes = await registry.live_addresses()
    ring = ConsistentHashRing(nodes)
    nodes = ring.get_nodes(key, REPLICATION_FACTOR)
    if not nodes:
        raise HTTPException(status_code=503, detail="No cache nodes available")

    failures = 0
    with LATENCY.labels(operation="delete").time():
        for node in nodes:
            try:
                response = await _grpc_delete(node, key)
                if not response.ok:
                    failures += 1
            except Exception:
                failures += 1

    if failures == len(nodes):
        REQUEST_FAILURES.labels(operation="delete").inc()
        raise HTTPException(status_code=503, detail="Delete failed on all replicas")
    return {"key": key, "replicas": nodes, "failures": failures}
