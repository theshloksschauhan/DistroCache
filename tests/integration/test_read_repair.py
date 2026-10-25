"""Read repair converges a stale replica after a single read."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from tests.integration.harness import Cluster, require, wait_until

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]


def test_a_read_repairs_a_stale_replica(cluster_factory: ClusterFactory) -> None:
    # Long DEAD_TIMEOUT: the killed node must stay a ring member so the ring never changes.
    cluster = cluster_factory(nodes=3, dead_timeout=60)
    key = "repair-me"
    stale, *others = cluster.handles_for(key)
    cluster.write(key, "v1")

    cluster.kill(stale)
    cluster.write(key, "v2")  # reaches only the two live replicas (W=2)
    for handle in others:
        entry = cluster.node_entry(handle, key)
        require(entry is not None and entry[0] == "v2", f"{handle.node_id} holds {entry}")

    rebalances = cluster.rebalance_count(node_id=stale.node_id)
    cluster.restart(stale)
    # The restarted node's own rebalance only pushes its stale v1, which the others reject.
    cluster.wait_for_rebalance_after(rebalances, node_id=stale.node_id)
    before = cluster.node_entry(stale, key)
    require(before is not None and before[0] == "v1", f"stale copy is {before}, expected v1")

    repairs_before = cluster.metric("distrocache_read_repairs_total")
    response = cluster.get(key)
    require(
        response.status_code == 200 and response.json()["value"] == "v2",
        f"GET returned {response.status_code}: {response.text}",
    )

    # The read's background repair brings the stale replica up to the newest version.
    def repaired() -> bool:
        entry = cluster.node_entry(stale, key)
        return entry is not None and entry[0] == "v2"

    wait_until(repaired, 10, "the stale replica to be repaired by the read")
    assert cluster.metric("distrocache_read_repairs_total") > repairs_before, (
        "a read of a stale replica did not increment distrocache_read_repairs_total"
    )
