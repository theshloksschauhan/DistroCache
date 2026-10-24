"""How the coordinator behaves when replicas are unreachable."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from tests.integration.harness import Cluster, require

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Stage 2: a gRPC error from one replica aborts the GET loop with a 500",
)
def test_get_succeeds_when_first_replica_is_dead(cluster_factory: ClusterFactory) -> None:
    cluster = cluster_factory(nodes=3)
    first = cluster.nodes["node1"]
    key = cluster.key_owned_first_by(first)
    cluster.write(key, "v1")

    cluster.kill(first)
    # The dead node must still be in the ring, otherwise the GET never contacts it.
    require(cluster.node_status(first) == "up", "membership already dropped the killed node")
    require(cluster.preference_list(key)[0] == first.address, "killed node is not read first")

    response = cluster.get(key)

    assert response.status_code == 200, (
        f"GET with 1 of 3 replicas dead returned {response.status_code}: {response.text}"
    )
    assert response.json()["value"] == "v1"


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Stage 2: replica errors escape as a 500 without incrementing the failure metric",
)
def test_failed_get_is_counted(cluster_factory: ClusterFactory) -> None:
    cluster = cluster_factory(nodes=3)
    cluster.write("counted", "v1")
    for handle in cluster.handles_for("counted"):
        cluster.kill(handle)
    before = cluster.metric("distrocache_request_failures_total", operation="get")

    response = cluster.get("counted")

    require(response.status_code != 200, "GET succeeded with every replica dead")
    after = cluster.metric("distrocache_request_failures_total", operation="get")
    assert after == before + 1, (
        f"GET failed with {response.status_code} but distrocache_request_failures_total"
        f'{{operation="get"}} went from {before} to {after}'
    )
