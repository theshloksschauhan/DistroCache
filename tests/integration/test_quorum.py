"""Read and write quorums (target: N=3, R=2, W=2)."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from tests.integration.harness import Cluster, require

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Stage 4: a write succeeds as soon as any single replica acknowledges it (W=1)",
)
def test_write_without_quorum_is_rejected(cluster_factory: ClusterFactory) -> None:
    cluster = cluster_factory(nodes=3)
    replicas = cluster.handles_for("quorum")
    for handle in replicas[1:]:
        cluster.kill(handle)
    require(
        all(cluster.node_status(h) == "up" for h in replicas), "membership dropped killed nodes"
    )

    response = cluster.put("quorum", "v1")

    assert response.status_code == 503, (
        f"PUT reached 1 of 3 replicas and returned {response.status_code}: {response.text}"
    )
