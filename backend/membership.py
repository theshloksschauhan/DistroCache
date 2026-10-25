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
    # health_ok is the last gRPC health probe result: None until first probed, else the result.
    health_ok: bool | None = None

    def status(self, now: float, heartbeat_timeout: float) -> str:
        """Liveness for routing and display, derived from the probe and the heartbeat age."""
        if self.health_ok is False:
            return "down"
        if now - self.last_seen > heartbeat_timeout:
            return "suspect"
        return "up"


class MembershipRegistry:
    """Ring membership (which addresses are in the hash ring) kept apart from liveness.

    Records are keyed by address, the stable identity in the ring. `ring_version` bumps only when
    the set of ring members changes (a new address registers, a member deregisters, or a member
    is removed after DEAD_TIMEOUT). Liveness changes never bump it, so a short outage does not
    rebalance the cluster.
    """

    def __init__(
        self,
        heartbeat_timeout: float,
        dead_timeout: float,
        seed_nodes: list[str] | None = None,
    ) -> None:
        self._heartbeat_timeout = heartbeat_timeout
        self._dead_timeout = dead_timeout
        self._lock = asyncio.Lock()
        self._nodes: dict[str, NodeRecord] = {}
        self._ring_version = 0
        if seed_nodes:
            now = time.time()
            for address in seed_nodes:
                # A seed is a placeholder record a real registration replaces in place, so an
                # address never gets a second record.
                self._nodes[address] = NodeRecord("seed", address, now, 0)

    async def register(self, node_id: str, address: str, key_count: int) -> int:
        async with self._lock:
            now = time.time()
            is_new_member = address not in self._nodes
            self._nodes[address] = NodeRecord(node_id, address, now, key_count, health_ok=True)
            if is_new_member:
                self._ring_version += 1  # a new address joined the ring
            return self._ring_version

    async def heartbeat(self, node_id: str, address: str, key_count: int) -> int:
        async with self._lock:
            now = time.time()
            record = self._nodes.get(address)
            if record is None:
                self._nodes[address] = NodeRecord(node_id, address, now, key_count, health_ok=True)
                self._ring_version += 1
                return self._ring_version
            # A heartbeat is evidence of life: refresh and clear any stale 'down' optimistically;
            # the health loop reconfirms. This does not bump ring_version.
            record.node_id = node_id
            record.last_seen = now
            record.key_count = key_count
            record.health_ok = True
            return self._ring_version

    async def deregister(self, address: str) -> int:
        async with self._lock:
            if address in self._nodes:
                del self._nodes[address]
                self._ring_version += 1
            return self._ring_version

    async def set_health(self, address: str, ok: bool) -> None:
        """Record the latest gRPC health probe result; drives the 'down' status for routing."""
        async with self._lock:
            record = self._nodes.get(address)
            if record is not None:
                record.health_ok = ok

    async def prune_expired(self) -> list[NodeRecord]:
        """Remove members gone longer than DEAD_TIMEOUT; that (ring) change bumps ring_version."""
        async with self._lock:
            now = time.time()
            removed = [
                record
                for record in self._nodes.values()
                if now - record.last_seen > self._dead_timeout
            ]
            for record in removed:
                del self._nodes[record.address]
            if removed:
                self._ring_version += 1
            return list(self._nodes.values())

    async def snapshot(self) -> list[tuple[NodeRecord, str]]:
        """Every record with its current status."""
        async with self._lock:
            now = time.time()
            return [
                (record, record.status(now, self._heartbeat_timeout))
                for record in self._nodes.values()
            ]

    async def ring_addresses(self) -> list[str]:
        """Every ring member's address, regardless of liveness (the stable preference set)."""
        async with self._lock:
            return [record.address for record in self._nodes.values()]

    async def down_addresses(self) -> set[str]:
        """Addresses the coordinator should skip (and count as failed votes) right now."""
        async with self._lock:
            return {record.address for record in self._nodes.values() if record.health_ok is False}

    async def ring_version(self) -> int:
        async with self._lock:
            return self._ring_version
