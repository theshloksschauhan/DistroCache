"""Version ordering across replicas."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from tests.integration.harness import Cluster, require

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]


def test_rebalance_does_not_replace_newer_value_with_older(cluster_factory: ClusterFactory) -> None:
    # Long timeout: the killed node must stay "up" so the ring (and ring_version) never changes.
    cluster = cluster_factory(nodes=3, heartbeat_timeout=60)
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

    ring_version = cluster.ring_version()
    rebalances = cluster.rebalance_count()
    cluster.restart(returning)
    # The restarted node's first heartbeat always triggers its own rebalance, which pushes
    # its ("v3", 3) copy to the other owners. That push is the event under test.
    cluster.wait_for_rebalance_after(rebalances)
    # Same ring_version means only the restarted node rebalanced.
    require(cluster.ring_version() == ring_version, "ring changed; other nodes may have rebalanced")

    for handle in others:
        entry = cluster.node_entry(handle, key)
        require(entry is not None, f"{handle.node_id} lost {key}")
        assert entry[0] == "v4", (
            f"{handle.node_id} now holds {entry[0]!r}: the restarted node's rebalance replaced "
            "the newest write with an older one"
        )
