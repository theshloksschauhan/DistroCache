from __future__ import annotations

import asyncio
from collections.abc import Sequence

import grpc

from .generated import cache_pb2_grpc


def rpc_error_text(exc: BaseException) -> str:
    """One line for logs and the 503 body. AioRpcError's own str() is a debug dump."""
    code = getattr(exc, "code", None)
    details = getattr(exc, "details", None)
    if callable(code) and callable(details):
        try:
            name = getattr(code(), "name", None)
            if isinstance(name, str):
                return f"{name}: {details()}"
        except Exception:
            pass
    text = str(exc).strip() or type(exc).__name__
    return f"{type(exc).__name__}: {text}"


class GrpcClientPool:
    def __init__(self, channel_options: Sequence[tuple[str, int]] | None = None) -> None:
        self._channels: dict[str, grpc.aio.Channel] = {}
        self._stubs: dict[str, cache_pb2_grpc.CacheNodeStub] = {}
        self._lock = asyncio.Lock()
        self._channel_options = tuple(channel_options or ())

    async def get_stub(self, target: str) -> cache_pb2_grpc.CacheNodeStub:
        async with self._lock:
            if target in self._stubs:
                return self._stubs[target]
            # None keeps the gRPC defaults. An empty tuple is not quite the same call.
            channel = grpc.aio.insecure_channel(target, options=self._channel_options or None)
            stub = cache_pb2_grpc.CacheNodeStub(channel)
            self._channels[target] = channel
            self._stubs[target] = stub
            return stub

    async def close(self) -> None:
        for channel in self._channels.values():
            await channel.close()
        self._channels.clear()
        self._stubs.clear()
