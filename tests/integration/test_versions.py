"""Version ordering across replicas."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from tests.integration.harness import Cluster, require

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]


def test_rebalance_does_not_replace_newer_value_with_older(cluster_factory: ClusterFactory) -> None:
    # Long DEAD_TIMEOUT: the killed node must stay a ring member so the ring never changes.
    cluster = cluster_factory(nodes=3, dead_timeout=60)
    key = "contested"
    returning, *others = cluster.handles_for(key)
    for value in ("v1", "v2", "v3"):
        cluster.write(key, value)  # every replica's counter for the key is now 3

    cluster.kill(returning)
    require(cluster.delete(key).status_code == 200, "setup DELETE failed")
    cluster.write(key, "v4")  # live replicas start a fresh row: counter 1
    for handle in others:
        entry = cluster.node_entry(handle, key)
        require(entry is not None and entry[0] == "v4", f"{handle.node_id} holds {entry}")

    rebalances = cluster.rebalance_count(node_id=returning.node_id)
    cluster.restart(returning)
    # The restarted node's first heartbeat always triggers its own rebalance, which pushes its
    # older copy to the other owners. Wait for that node's own rebalance via its node_id label;
    # that push is the event under test.
    cluster.wait_for_rebalance_after(rebalances, node_id=returning.node_id)

    for handle in others:
        entry = cluster.node_entry(handle, key)
        require(entry is not None, f"{handle.node_id} lost {key}")
        assert entry[0] == "v4", (
            f"{handle.node_id} now holds {entry[0]!r}: the restarted node's rebalance replaced "
            "the newest write with an older one"
        )
