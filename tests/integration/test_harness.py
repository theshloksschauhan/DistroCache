"""Self-tests for the harness: if these fail, no other integration result can be trusted."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from tests.integration.harness import Cluster

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]


def test_cluster_serves_writes_and_reads(cluster_factory: ClusterFactory) -> None:
    cluster = cluster_factory(nodes=3)

    cluster.write("alpha", "1")  # also checks the coordinator used the harness's replica order

    response = cluster.get("alpha")
    assert response.status_code == 200
    assert response.json()["value"] == "1"
    for handle in cluster.handles_for("alpha"):
        assert cluster.node_entry(handle, "alpha") == ("1", 1), handle.node_id


def test_acknowledged_write_survives_hard_kill_and_restart(cluster_factory: ClusterFactory) -> None:
    cluster = cluster_factory(nodes=3)
    cluster.write("durable", "kept")
    victim = cluster.handles_for("durable")[0]

    cluster.kill(victim)
    assert not victim.process.running
    cluster.restart(victim)

    assert cluster.node_entry(victim, "durable") == ("kept", 1), (
        "a write acknowledged before kill() was lost from the node's SQLite file"
    )


def test_graceful_stop_deregisters_and_restart_rejoins(cluster_factory: ClusterFactory) -> None:
    cluster = cluster_factory(nodes=3)
    handle = cluster.nodes["node2"]

    exit_code = cluster.stop(handle)

    assert exit_code == 0, f"node exited with {exit_code} on a graceful stop"
    assert cluster.node_status(handle) is None, "graceful stop did not deregister the node"
    log = handle.process.log_text()
    assert "node node2 stopped" in log
    assert "Traceback" not in log
    assert "Task was destroyed but it is pending" not in log

    cluster.restart(handle)
    assert cluster.node_status(handle) == "up"
