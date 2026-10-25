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


def test_seeded_nodes_have_one_live_record_each(cluster_factory: ClusterFactory) -> None:
    cluster = cluster_factory(nodes=3, seed_nodes=True, heartbeat_timeout=HEARTBEAT_TIMEOUT)

    # Records are keyed by address, so a real registration replaces the matching seed in place
    # rather than adding a second record. A fixed wait on purpose, not polling: the correct
    # behaviour is that nothing changes (no record goes stale, none duplicates), and an absence
    # has no event to poll for, so the test sits out the whole window. 2x the timeout covers the
    # prune interval and scheduling delays.
    time.sleep(HEARTBEAT_TIMEOUT * 2)
    records = cluster.membership()["nodes"]

    per_address = Counter(record["address"] for record in records)
    duplicates = {address: n for address, n in per_address.items() if n > 1}
    assert not duplicates, f"addresses with more than one membership record: {records}"
    not_up = [record for record in records if record["status"] != "up"]
    assert not not_up, f"live seeded nodes reported as not up: {not_up}"
