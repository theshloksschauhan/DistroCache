from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Tuple

import httpx

logger = logging.getLogger("distrocache.node.membership")


class MembershipClient:
    def __init__(self, base_url: str, node_id: str, address: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._node_id = node_id
        self._address = address
        self._client = httpx.AsyncClient(timeout=2.0)

    async def register(self, key_count: int) -> Tuple[int, List[str]]:
        payload = {"node_id": self._node_id, "address": self._address, "key_count": key_count}
        response = await self._client.post(f"{self._base_url}/membership/register", json=payload)
        response.raise_for_status()
        data: Dict[str, Any] = response.json()
        return int(data.get("ring_version", 0)), list(data.get("nodes", []))

    async def heartbeat(self, key_count: int) -> Tuple[int, List[str]]:
        payload = {"node_id": self._node_id, "address": self._address, "key_count": key_count}
        response = await self._client.post(f"{self._base_url}/membership/heartbeat", json=payload)
        response.raise_for_status()
        data: Dict[str, Any] = response.json()
        return int(data.get("ring_version", 0)), list(data.get("nodes", []))

    async def deregister(self) -> None:
        payload = {"node_id": self._node_id}
        response = await self._client.post(f"{self._base_url}/membership/deregister", json=payload)
        response.raise_for_status()

    async def report_rebalance(self, duration_s: float, keys_moved: int, replicas_restored: int) -> None:
        payload = {
            "node_id": self._node_id,
            "duration_s": duration_s,
            "keys_moved": keys_moved,
            "replicas_restored": replicas_restored,
        }
        try:
            response = await self._client.post(f"{self._base_url}/membership/rebalance", json=payload)
            response.raise_for_status()
        except Exception as exc:
            logger.warning("rebalance report failed: %s", exc)

    async def close(self) -> None:
        await self._client.aclose()


class HeartbeatLoop:
    def __init__(
        self,
        membership: MembershipClient,
        interval_s: float,
    ) -> None:
        self._membership = membership
        self._interval_s = interval_s
        self._stop = asyncio.Event()
        self._ring_version = 0
        self._nodes: List[str] = []

    async def start(self, key_count: int) -> Tuple[int, List[str]]:
        self._ring_version, self._nodes = await self._membership.register(key_count)
        return self._ring_version, self._nodes

    async def run(self, key_count_fn) -> None:
        while not self._stop.is_set():
            try:
                key_count = await key_count_fn()
                self._ring_version, self._nodes = await self._membership.heartbeat(key_count)
            except Exception as exc:
                logger.warning("heartbeat failed: %s", exc)
            await asyncio.sleep(self._interval_s)

    def stop(self) -> None:
        self._stop.set()

    @property
    def ring_version(self) -> int:
        return self._ring_version

    @property
    def nodes(self) -> List[str]:
        return self._nodes
