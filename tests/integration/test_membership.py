"""Membership records."""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable

import pytest

from tests.integration.harness import Cluster

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]

HEARTBEAT_TIMEOUT = 2.0


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Stage 7: seeds are stored as seed-N records next to the node's own record",
)
def test_seeded_nodes_have_one_live_record_each(cluster_factory: ClusterFactory) -> None:
    cluster = cluster_factory(nodes=3, seed_nodes=True, heartbeat_timeout=HEARTBEAT_TIMEOUT)

    # A fixed wait on purpose, not polling. Seed records get last_seen at coordinator start
    # and never heartbeat, so they can only go down once HEARTBEAT_TIMEOUT has passed and the
    # prune loop (every 0.2 s) has run. The correct behaviour is that nothing changes, and
    # an absence has no event to poll for: the test must sit out the whole window. 2x the
    # timeout covers the prune interval and scheduling delays.
    time.sleep(HEARTBEAT_TIMEOUT * 2)
    records = cluster.membership()["nodes"]

    per_address = Counter(record["address"] for record in records)
    duplicates = {address: n for address, n in per_address.items() if n > 1}
    assert not duplicates, f"addresses with more than one membership record: {records}"
    not_up = [record for record in records if record["status"] != "up"]
    assert not not_up, f"live seeded nodes reported as not up: {not_up}"
