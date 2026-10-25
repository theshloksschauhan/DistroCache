"""Deletes must win over copies held by replicas that missed them."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from tests.integration.harness import Cluster, require

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]


def test_delete_is_not_undone_by_a_replica_that_missed_it(cluster_factory: ClusterFactory) -> None:
    cluster = cluster_factory(nodes=3, dead_timeout=60)
    key = "deleted"
    missed = cluster.handles_for(key)[0]
    cluster.write(key, "v1")

    cluster.kill(missed)
    require(cluster.delete(key).status_code == 200, "setup DELETE failed")

    rebalances = cluster.rebalance_count(node_id=missed.node_id)
    cluster.restart(missed)
    # Let the restarted node's own startup rebalance finish so the GET sees a settled state.
    cluster.wait_for_rebalance_after(rebalances, node_id=missed.node_id)

    response = cluster.get(key)

    # Anything but 200/404 is a different failure (see test_failures.py), not resurrection.
    require(response.status_code in (200, 404), f"GET failed: {response.status_code}")
    assert response.status_code == 404, f"deleted key came back: {response.text}"
