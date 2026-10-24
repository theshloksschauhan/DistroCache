from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass


@dataclass
class NodeRecord:
    node_id: str
    address: str
    last_seen: float
    key_count: int
    status: str


class MembershipRegistry:
    def __init__(self, timeout_seconds: float, seed_nodes: list[str] | None = None) -> None:
        self._timeout_seconds = timeout_seconds
        self._lock = asyncio.Lock()
        self._nodes: dict[str, NodeRecord] = {}
        self._ring_version = 0
        if seed_nodes:
            now = time.time()
            for idx, address in enumerate(seed_nodes):
                node_id = f"seed-{idx + 1}"
                self._nodes[node_id] = NodeRecord(
                    node_id=node_id,
                    address=address,
                    last_seen=now,
                    key_count=0,
                    status="up",
                )

    async def register(self, node_id: str, address: str, key_count: int) -> int:
        async with self._lock:
            existing = self._nodes.get(node_id)
            now = time.time()
            if existing is None or existing.address != address or existing.status != "up":
                self._ring_version += 1
            self._nodes[node_id] = NodeRecord(
                node_id=node_id,
                address=address,
                last_seen=now,
                key_count=key_count,
                status="up",
            )
            return self._ring_version

    async def heartbeat(self, node_id: str, address: str, key_count: int) -> int:
        async with self._lock:
            now = time.time()
            record = self._nodes.get(node_id)
            if record is None:
                self._nodes[node_id] = NodeRecord(
                    node_id=node_id,
                    address=address,
                    last_seen=now,
                    key_count=key_count,
                    status="up",
                )
                self._ring_version += 1
                return self._ring_version
            record.last_seen = now
            record.address = address
            record.key_count = key_count
            if record.status != "up":
                record.status = "up"
                self._ring_version += 1
            return self._ring_version

    async def deregister(self, node_id: str) -> int:
        async with self._lock:
            if node_id in self._nodes:
                self._nodes.pop(node_id)
                self._ring_version += 1
            return self._ring_version

    async def prune_expired(self) -> list[NodeRecord]:
        async with self._lock:
            now = time.time()
            changed = False
            for record in list(self._nodes.values()):
                if record.status == "up" and now - record.last_seen > self._timeout_seconds:
                    record.status = "down"
                    changed = True
            if changed:
                self._ring_version += 1
            return list(self._nodes.values())

    async def snapshot(self) -> list[NodeRecord]:
        async with self._lock:
            return list(self._nodes.values())

    async def live_addresses(self) -> list[str]:
        async with self._lock:
            return [record.address for record in self._nodes.values() if record.status == "up"]

    async def ring_version(self) -> int:
        async with self._lock:
            return self._ring_version
