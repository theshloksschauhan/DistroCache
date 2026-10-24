from __future__ import annotations

import asyncio
from typing import Dict

import grpc

from .generated import cache_pb2_grpc


class GrpcClientPool:
    def __init__(self) -> None:
        self._channels: Dict[str, grpc.aio.Channel] = {}
        self._stubs: Dict[str, cache_pb2_grpc.CacheNodeStub] = {}
        self._lock = asyncio.Lock()

    async def get_stub(self, target: str) -> cache_pb2_grpc.CacheNodeStub:
        async with self._lock:
            if target in self._stubs:
                return self._stubs[target]
            channel = grpc.aio.insecure_channel(target)
            stub = cache_pb2_grpc.CacheNodeStub(channel)
            self._channels[target] = channel
            self._stubs[target] = stub
            return stub

    async def close(self) -> None:
        for channel in self._channels.values():
            await channel.close()
        self._channels.clear()
        self._stubs.clear()
