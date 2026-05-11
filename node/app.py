from __future__ import annotations

import asyncio
import logging
import os
import signal

import grpc

from .generated import cache_pb2, cache_pb2_grpc
from .storage import Storage

NODE_ID = os.getenv("NODE_ID", "node-unknown")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
REDIS_URL = os.getenv("REDIS_URL")
TTL_SECONDS = int(os.getenv("TTL_SECONDS", "0"))
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "0"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("distrocache.node")

storage = Storage(
    redis_url=REDIS_URL,
    ttl_seconds=TTL_SECONDS if TTL_SECONDS > 0 else None,
    max_items=MAX_ITEMS if MAX_ITEMS > 0 else None,
)


class CacheNodeService(cache_pb2_grpc.CacheNodeServicer):
    async def Get(self, request: cache_pb2.GetRequest, context: grpc.aio.ServicerContext) -> cache_pb2.GetResponse:
        value = storage.get(request.key)
        if value is None:
            return cache_pb2.GetResponse(found=False)
        return cache_pb2.GetResponse(found=True, value=value)

    async def Put(self, request: cache_pb2.PutRequest, context: grpc.aio.ServicerContext) -> cache_pb2.PutResponse:
        try:
            storage.put(request.key, request.value)
            return cache_pb2.PutResponse(ok=True)
        except Exception as exc:
            return cache_pb2.PutResponse(ok=False, error=str(exc))

    async def Delete(self, request: cache_pb2.DeleteRequest, context: grpc.aio.ServicerContext) -> cache_pb2.DeleteResponse:
        try:
            ok = storage.delete(request.key)
            return cache_pb2.DeleteResponse(ok=ok)
        except Exception as exc:
            return cache_pb2.DeleteResponse(ok=False, error=str(exc))

    async def Health(self, request: cache_pb2.HealthRequest, context: grpc.aio.ServicerContext) -> cache_pb2.HealthResponse:
        return cache_pb2.HealthResponse(node_id=NODE_ID, ok=True)


async def serve() -> None:
    server = grpc.aio.server()
    cache_pb2_grpc.add_CacheNodeServicer_to_server(CacheNodeService(), server)
    server.add_insecure_port(f"0.0.0.0:{GRPC_PORT}")
    await server.start()
    logger.info("node %s listening on %s", NODE_ID, GRPC_PORT)

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
    await server.stop(grace=2)


if __name__ == "__main__":
    asyncio.run(serve())
