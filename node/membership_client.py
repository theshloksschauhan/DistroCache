from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger("distrocache.node.membership")


class MembershipClient:
    def __init__(self, base_url: str, node_id: str, address: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._node_id = node_id
        self._address = address
        self._client = httpx.AsyncClient(timeout=2.0)

    async def register(self, key_count: int) -> tuple[int, list[str]]:
        payload = {"node_id": self._node_id, "address": self._address, "key_count": key_count}
        response = await self._client.post(f"{self._base_url}/membership/register", json=payload)
        response.raise_for_status()
        data: dict[str, Any] = response.json()
        return int(data.get("ring_version", 0)), list(data.get("nodes", []))

    async def heartbeat(self, key_count: int) -> tuple[int, list[str]]:
        payload = {"node_id": self._node_id, "address": self._address, "key_count": key_count}
        response = await self._client.post(f"{self._base_url}/membership/heartbeat", json=payload)
        response.raise_for_status()
        data: dict[str, Any] = response.json()
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
