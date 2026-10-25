"""Unit tests for the coordinator's MembershipRegistry.

The registry keeps ring membership (which addresses own keys) apart from liveness (up/suspect/
down). These were previously exercised only through the integration harness; here each behaviour
is pinned directly. Each test runs its operations inside one ``asyncio.run`` scenario so the
registry's lock lives on a single event loop.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable
from typing import TypeVar

from backend.membership import MembershipRegistry, NodeRecord

T = TypeVar("T")


def run(coro: Awaitable[T]) -> T:
    return asyncio.run(coro)  # type: ignore[arg-type]


def test_register_bumps_ring_version_only_for_a_new_address() -> None:
    async def scenario() -> None:
        reg = MembershipRegistry(heartbeat_timeout=5, dead_timeout=30)
        assert await reg.ring_version() == 0
        assert await reg.register("n1", "127.0.0.1:1", 0) == 1
        # Re-registering the same address (e.g. after a node restart) must not bump the ring.
        assert await reg.register("n1", "127.0.0.1:1", 3) == 1
        assert await reg.register("n2", "127.0.0.1:2", 0) == 2
        assert set(await reg.ring_addresses()) == {"127.0.0.1:1", "127.0.0.1:2"}

    run(scenario())


def test_register_replaces_a_seed_in_place() -> None:
    async def scenario() -> None:
        reg = MembershipRegistry(heartbeat_timeout=5, dead_timeout=30, seed_nodes=["127.0.0.1:1"])
        # The seed already occupies the ring slot, so a real registration does not add a member.
        assert await reg.ring_version() == 0
        assert await reg.register("n1", "127.0.0.1:1", 0) == 0
        records = {r.address: r for r, _ in await reg.snapshot()}
        assert records["127.0.0.1:1"].node_id == "n1"

    run(scenario())


def test_heartbeat_adds_unknown_and_refreshes_known() -> None:
    async def scenario() -> None:
        reg = MembershipRegistry(heartbeat_timeout=5, dead_timeout=30)
        # A heartbeat from an address the coordinator never saw register it (relearn after a
        # coordinator restart) adds it and bumps the ring.
        assert await reg.heartbeat("n1", "127.0.0.1:1", 2) == 1
        # A heartbeat from a known address refreshes it without bumping the ring.
        assert await reg.heartbeat("n1", "127.0.0.1:1", 9) == 1
        records = {r.address: r for r, _ in await reg.snapshot()}
        assert records["127.0.0.1:1"].key_count == 9

    run(scenario())


def test_deregister_removes_and_bumps_only_when_present() -> None:
    async def scenario() -> None:
        reg = MembershipRegistry(heartbeat_timeout=5, dead_timeout=30)
        await reg.register("n1", "127.0.0.1:1", 0)
        assert await reg.deregister("127.0.0.1:1") == 2  # removal bumps the ring
        assert await reg.deregister("127.0.0.1:1") == 2  # already gone: no bump
        assert await reg.ring_addresses() == []

    run(scenario())


def test_set_health_drives_down_addresses_and_heartbeat_clears_it() -> None:
    async def scenario() -> None:
        reg = MembershipRegistry(heartbeat_timeout=5, dead_timeout=30)
        await reg.register("n1", "127.0.0.1:1", 0)
        assert await reg.down_addresses() == set()
        await reg.set_health("127.0.0.1:1", False)
        assert await reg.down_addresses() == {"127.0.0.1:1"}
        # A fresh heartbeat is evidence of life and optimistically clears the down flag.
        await reg.heartbeat("n1", "127.0.0.1:1", 0)
        assert await reg.down_addresses() == set()
        # set_health on an unknown address is a no-op (does not create a record).
        await reg.set_health("127.0.0.1:9", False)
        assert "127.0.0.1:9" not in await reg.ring_addresses()

    run(scenario())


def test_status_reflects_health_then_heartbeat_age() -> None:
    now = time.time()
    fresh = NodeRecord("n", "a", last_seen=now, key_count=0, health_ok=True)
    assert fresh.status(now, heartbeat_timeout=5) == "up"
    stale = NodeRecord("n", "a", last_seen=now - 100, key_count=0, health_ok=None)
    assert stale.status(now, heartbeat_timeout=5) == "suspect"
    unhealthy = NodeRecord("n", "a", last_seen=now, key_count=0, health_ok=False)
    # health_ok False wins over a fresh heartbeat.
    assert unhealthy.status(now, heartbeat_timeout=5) == "down"


def test_prune_expired_removes_only_stale_members_and_bumps_once() -> None:
    async def scenario() -> None:
        # Negative dead_timeout => every member is unconditionally past the window, so the test
        # does not depend on wall-clock resolution (time.time() is coarse on Windows).
        reg = MembershipRegistry(heartbeat_timeout=5, dead_timeout=-1.0)
        await reg.register("n1", "127.0.0.1:1", 0)
        await reg.register("n2", "127.0.0.1:2", 0)
        version_before = await reg.ring_version()
        survivors = await reg.prune_expired()
        assert survivors == []
        assert await reg.ring_version() == version_before + 1
        # A second prune with nothing to remove does not bump again.
        await reg.prune_expired()
        assert await reg.ring_version() == version_before + 1

    run(scenario())


def test_snapshot_pairs_each_record_with_its_status() -> None:
    async def scenario() -> None:
        reg = MembershipRegistry(heartbeat_timeout=100, dead_timeout=30)
        await reg.register("n1", "127.0.0.1:1", 0)
        await reg.set_health("127.0.0.1:1", False)
        snapshot = await reg.snapshot()
        assert len(snapshot) == 1
        record, status = snapshot[0]
        assert record.address == "127.0.0.1:1" and status == "down"

    run(scenario())
