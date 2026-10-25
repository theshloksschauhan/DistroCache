"""Read and write quorums (target: N=3, R=2, W=2)."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from tests.integration.harness import Cluster, require

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]


def test_write_without_quorum_is_rejected(cluster_factory: ClusterFactory) -> None:
    cluster = cluster_factory(nodes=3)
    replicas = cluster.handles_for("quorum")
    for handle in replicas[1:]:
        cluster.kill(handle)
    # The killed nodes stay ring members (the coordinator skips them as failed votes), so the
    # write still targets all three and only one can ack.
    require(all(cluster.is_member(h) for h in replicas), "membership dropped killed nodes")

    response = cluster.put("quorum", "v1")

    assert response.status_code == 503, (
        f"PUT reached 1 of 3 replicas and returned {response.status_code}: {response.text}"
    )


def test_stale_replica_does_not_hide_newer_value(cluster_factory: ClusterFactory) -> None:
    # Long DEAD_TIMEOUT: the killed node must stay a ring member so the ring never changes.
    cluster = cluster_factory(nodes=3, dead_timeout=60)
    stale = cluster.nodes["node1"]
    key = cluster.key_owned_first_by(stale)
    others = cluster.handles_for(key)[1:]
    cluster.write(key, "old")

    cluster.kill(stale)
    cluster.write(key, "new")  # reaches only the two live replicas

    rebalances = cluster.rebalance_count(node_id=stale.node_id)
    cluster.restart(stale)
    # A restarted node always rebalances once on its first heartbeat. Wait for that node's own
    # rebalance (via its node_id label), so it can't race the GET. It only pushes the stale
    # node's own copy, which peers reject.
    cluster.wait_for_rebalance_after(rebalances, node_id=stale.node_id)
    stale_entry = cluster.node_entry(stale, key)
    require(stale_entry is not None and stale_entry[0] == "old", f"stale copy is {stale_entry}")
    for handle in others:
        entry = cluster.node_entry(handle, key)
        require(entry is not None and entry[0] == "new", f"{handle.node_id} holds {entry}")
    require(cluster.preference_list(key)[0] == stale.address, "stale node is not read first")

    response = cluster.get(key)

    require(response.status_code == 200, f"GET failed: {response.text}")
    body = response.json()
    assert body["value"] == "new", f"GET returned stale {body['value']!r} from {body.get('node')}"
