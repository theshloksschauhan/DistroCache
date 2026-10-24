"""Self-tests for the harness: if these fail, no other integration result can be trusted."""

from __future__ import annotations

import socket
from collections.abc import Callable, Iterator

import pytest

from tests.integration import harness
from tests.integration.harness import PORT_ATTEMPTS, Cluster, HarnessError

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]


def _listen_on(port: int) -> socket.socket:
    """Hold a localhost port with a listening socket so that binding it fails."""
    sock = socket.socket()
    if not harness.IS_WINDOWS:
        # Allow re-binding a port a just-killed process left in TIME_WAIT. (On Windows this
        # option would let a later bind steal the port; there TIME_WAIT doesn't block us.)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", port))
    sock.listen()
    return sock


@pytest.fixture
def held_port() -> Iterator[int]:
    sock = _listen_on(0)
    yield int(sock.getsockname()[1])
    sock.close()


def _hand_out(monkeypatch: pytest.MonkeyPatch, *planned: int | None) -> None:
    """Make free_port() return `planned` in order (None = a real free port), then real ones."""
    real = harness.free_port
    queue = list(planned)
    monkeypatch.setattr(harness, "free_port", lambda: (queue.pop(0) if queue else None) or real())


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


# start() picks node ports first, then the coordinator's.


def test_node_retries_on_a_new_port_when_its_port_is_taken(
    cluster_factory: ClusterFactory, monkeypatch: pytest.MonkeyPatch, held_port: int
) -> None:
    _hand_out(monkeypatch, held_port)

    cluster = cluster_factory(nodes=1)

    node = cluster.nodes["node1"]
    assert node.port != held_port
    assert "failed to bind" in node.process.log_text().lower()
    cluster.write("moved", "ok")
    assert cluster.get("moved").json()["value"] == "ok"


def test_coordinator_retries_on_a_new_port_when_its_port_is_taken(
    cluster_factory: ClusterFactory, monkeypatch: pytest.MonkeyPatch, held_port: int
) -> None:
    _hand_out(monkeypatch, None, held_port)

    cluster = cluster_factory(nodes=1)

    assert not cluster.url.endswith(f":{held_port}")
    assert cluster.coordinator is not None
    assert "error while attempting to bind" in cluster.coordinator.log_text().lower()
    cluster.write("moved", "ok")


def test_gives_up_after_the_retry_limit(
    cluster_factory: ClusterFactory, monkeypatch: pytest.MonkeyPatch, held_port: int
) -> None:
    _hand_out(monkeypatch, None, *[held_port] * PORT_ATTEMPTS)

    with pytest.raises(HarnessError, match=f"after {PORT_ATTEMPTS} attempts"):
        cluster_factory(nodes=1)


def test_restart_does_not_move_a_node_to_a_new_port(
    cluster_factory: ClusterFactory,
) -> None:
    cluster = cluster_factory(nodes=1)
    node = cluster.nodes["node1"]
    cluster.kill(node)
    sock = _listen_on(node.port)
    try:
        with pytest.raises(HarnessError, match="must keep port"):
            cluster.restart(node)
    finally:
        sock.close()
