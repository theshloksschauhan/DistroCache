"""Keys survive membership changes once rebalancing settles."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from tests.integration.harness import Cluster

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]


def test_no_keys_lost_when_a_node_joins_and_another_leaves(
    cluster_factory: ClusterFactory,
) -> None:
    # RF=2 with 3-4 nodes, so ownership actually moves (with RF=3 on 3 nodes, nothing would).
    cluster = cluster_factory(nodes=3, replication_factor=2)
    expected = {f"jl-{i}": f"value-{i}" for i in range(200)}
    for key, value in expected.items():
        cluster.write(key, value)

    cluster.add_node()
    cluster.wait_for_rebalances_settled()
    cluster.stop(cluster.nodes["node2"])  # graceful leave: deregisters, ring shrinks
    cluster.wait_for_rebalances_settled()

    lost = {}
    for key, value in expected.items():
        response = cluster.get(key)
        if response.status_code != 200 or response.json()["value"] != value:
            lost[key] = (response.status_code, response.text)
    assert not lost, f"{len(lost)} of {len(expected)} keys lost, e.g. {list(lost.items())[:5]}"
