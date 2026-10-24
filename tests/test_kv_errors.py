"""Replica errors are votes. These tests describe Stage 2 and fail on the code before it."""

from __future__ import annotations

import asyncio
import json
import logging

import grpc
import pytest
from fastapi import HTTPException
from prometheus_client import generate_latest
from starlette.responses import JSONResponse

from backend.app import Coordinator, Settings, create_app, get_key, put_key
from distrocache_common.generated import cache_pb2
from tests.integration.harness import parse_metric


class StatusError(Exception):
    """Same surface as grpc.aio.AioRpcError: code() and details()."""

    def __init__(self, status: grpc.StatusCode, detail: str = "boom") -> None:
        super().__init__(detail)
        self._status = status
        self._detail = detail

    def code(self) -> grpc.StatusCode:
        return self._status

    def details(self) -> str:
        return self._detail


class ScriptedStub:
    def __init__(self, errors: list[BaseException]) -> None:
        self.errors = list(errors)
        self.calls = 0

    async def _answer(
        self, found_value: bytes | None
    ) -> cache_pb2.GetResponse | cache_pb2.PutResponse:
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)
        if found_value is None:
            return cache_pb2.PutResponse(ok=True)
        return cache_pb2.GetResponse(found=True, value=found_value)

    async def Get(self, request: cache_pb2.GetRequest, timeout: float) -> cache_pb2.GetResponse:
        response = await self._answer(b"v")
        assert isinstance(response, cache_pb2.GetResponse)
        return response

    async def Put(self, request: cache_pb2.PutRequest, timeout: float) -> cache_pb2.PutResponse:
        response = await self._answer(None)
        assert isinstance(response, cache_pb2.PutResponse)
        return response


class StubPool:
    def __init__(self, stub: ScriptedStub) -> None:
        self.stub = stub

    async def get_stub(self, target: str) -> ScriptedStub:
        return self.stub

    async def close(self) -> None:
        return None


def _coordinator() -> Coordinator:
    return Coordinator(Settings(node_addresses=[], retry_attempts=2, retry_backoff=0))


def _failures(operation: str, reason: str | None = None) -> float:
    labels = {"operation": operation}
    if reason is not None:
        labels["reason"] = reason
    return parse_metric(generate_latest().decode(), "distrocache_request_failures_total", **labels)


def _coordinator_for(*addresses: str, replication_factor: int = 3) -> Coordinator:
    # No lifespan here, so the health loop never dials these addresses.
    app = create_app(
        Settings(
            node_addresses=list(addresses),
            replication_factor=replication_factor,
            retry_attempts=2,
            retry_backoff=0,
        )
    )
    coordinator: Coordinator = app.state.coordinator
    return coordinator


def _invoke(coro: object) -> tuple[int, dict[str, object]]:
    """Run a route. An escaped replica error fails the test instead of counting as the bug."""
    try:
        result = asyncio.run(coro)  # type: ignore[arg-type]
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, dict) else {"detail": exc.detail}
        return exc.status_code, detail
    except Exception as exc:
        raise AssertionError(f"request raised {type(exc).__name__}: {exc}") from exc
    if isinstance(result, JSONResponse):
        return result.status_code, json.loads(bytes(result.body))
    return 200, result


@pytest.mark.parametrize("method", ["grpc_get", "grpc_put"])
def test_permanent_errors_are_not_retried(method: str) -> None:
    # Two errors so a retry would also fail. The assertion is that there is no retry.
    stub = ScriptedStub([StatusError(grpc.StatusCode.INTERNAL)] * 2)
    coordinator = _coordinator()
    coordinator.clients = StubPool(stub)  # type: ignore[assignment]
    call = getattr(coordinator, method)
    args = ("127.0.0.1:1", "k", b"v") if method == "grpc_put" else ("127.0.0.1:1", "k")

    with pytest.raises(StatusError):
        asyncio.run(call(*args))

    assert stub.calls == 1, f"{method} retried a permanent INTERNAL error ({stub.calls} calls)"


def test_get_uses_the_next_replica_when_the_first_errors() -> None:
    coordinator = _coordinator_for("127.0.0.1:1", "127.0.0.1:2", replication_factor=2)
    calls: list[str] = []

    async def fake_get(node: str, key: str) -> cache_pb2.GetResponse:
        calls.append(node)
        if len(calls) == 1:
            raise StatusError(grpc.StatusCode.UNAVAILABLE, "connection refused")
        return cache_pb2.GetResponse(found=True, value=b"v1")

    coordinator.grpc_get = fake_get  # type: ignore[method-assign]
    status, body = _invoke(get_key("k", coordinator))

    assert status == 200, body
    assert body["value"] == "v1"
    assert len(calls) == 2


def test_no_acks_is_503_and_counts_one_unavailable_failure() -> None:
    coordinator = _coordinator_for("127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3")

    async def fake_get(node: str, key: str) -> cache_pb2.GetResponse:
        raise RuntimeError("down")

    coordinator.grpc_get = fake_get  # type: ignore[method-assign]
    before = _failures("get", reason="unavailable")
    status, body = _invoke(get_key("k", coordinator))
    after = _failures("get", reason="unavailable")

    assert status == 503, body
    assert body.get("acks") == 0
    assert body.get("required") == 1
    errors = body.get("errors")
    assert isinstance(errors, list) and len(errors) == 3
    assert after == before + 1


def test_get_miss_does_not_count_as_a_request_failure() -> None:
    coordinator = _coordinator_for("127.0.0.1:1", replication_factor=1)

    async def missing(node: str, key: str) -> cache_pb2.GetResponse:
        return cache_pb2.GetResponse(found=False)

    coordinator.grpc_get = missing  # type: ignore[method-assign]
    before = _failures("get")
    status, _body = _invoke(get_key("missing", coordinator))
    after = _failures("get")

    assert status == 404
    assert after == before, "a GET miss incremented distrocache_request_failures_total"


def test_rejected_write_is_a_failed_vote() -> None:
    coordinator = _coordinator_for("127.0.0.1:9", replication_factor=1)

    async def rejected(node: str, key: str, value: bytes) -> cache_pb2.PutResponse:
        return cache_pb2.PutResponse(ok=False, error="disk full")

    coordinator.grpc_put = rejected  # type: ignore[method-assign]
    status, body = _invoke(put_key("k", {"value": "v"}, coordinator))

    assert status == 503, body
    assert body.get("acks") == 0
    assert body.get("required") == 1
    assert body.get("errors") == [{"node": "127.0.0.1:9", "error": "disk full"}]


@pytest.mark.parametrize("status", [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED])
@pytest.mark.parametrize("method", ["grpc_get", "grpc_put"])
def test_transient_errors_are_retried(method: str, status: grpc.StatusCode) -> None:
    # One failure then success: both attempts happen, including for writes.
    stub = ScriptedStub([StatusError(status, "try again")])
    coordinator = _coordinator()
    coordinator.clients = StubPool(stub)  # type: ignore[assignment]
    call = getattr(coordinator, method)
    args = ("127.0.0.1:1", "k", b"v") if method == "grpc_put" else ("127.0.0.1:1", "k")

    asyncio.run(call(*args))

    assert stub.calls == 2


def test_retries_stop_at_the_attempt_limit() -> None:
    stub = ScriptedStub([StatusError(grpc.StatusCode.UNAVAILABLE)] * 5)
    coordinator = _coordinator()
    coordinator.clients = StubPool(stub)  # type: ignore[assignment]

    with pytest.raises(StatusError):
        asyncio.run(coordinator.grpc_put("127.0.0.1:1", "k", b"v"))

    assert stub.calls == 2


def test_corrupt_value_counts_as_an_internal_failure() -> None:
    coordinator = _coordinator_for("127.0.0.1:1", replication_factor=1)

    async def corrupt(node: str, key: str) -> cache_pb2.GetResponse:
        return cache_pb2.GetResponse(found=True, value=b"\xff")

    coordinator.grpc_get = corrupt  # type: ignore[method-assign]
    before = _failures("get", reason="internal")
    status, body = _invoke(get_key("k", coordinator))

    assert status == 500, body
    assert _failures("get", reason="internal") == before + 1


def test_no_nodes_is_503_with_the_vote_body() -> None:
    coordinator = _coordinator_for()
    before = _failures("put", reason="unavailable")
    status, body = _invoke(put_key("k", {"value": "v"}, coordinator))

    assert status == 503, body
    assert body.get("acks") == 0
    assert body.get("required") == 1
    assert body.get("errors") == []
    assert _failures("put", reason="unavailable") == before + 1


def test_failed_vote_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    coordinator = _coordinator_for("127.0.0.1:9", replication_factor=1)

    async def down(node: str, key: str, value: bytes) -> cache_pb2.PutResponse:
        raise RuntimeError("unreachable")

    coordinator.grpc_put = down  # type: ignore[method-assign]
    caplog.set_level(logging.WARNING, logger="distrocache.api")
    _invoke(put_key("k", {"value": "v"}, coordinator))

    assert any(
        r.levelno == logging.WARNING and "127.0.0.1:9" in r.message and "unreachable" in r.message
        for r in caplog.records
    )
