"""Concurrent writes to one key must converge to a single value on every replica."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

import pytest

from tests.integration.harness import Cluster, require, wait_until

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]

NodeVersion = tuple[int, int, str]


def _version_tuple(version: dict[str, Any]) -> NodeVersion:
    return (version["physical_ms"], version["logical"], version["coordinator_id"])


def test_concurrent_writes_to_one_key_converge_on_all_replicas(
    cluster_factory: ClusterFactory,
) -> None:
    # N=3 so the single key lives on all three nodes; each write needs W=2 acks and the third
    # replica is caught up in the background, so every write eventually reaches every replica.
    cluster = cluster_factory(nodes=3)
    key = "hot-key"
    cluster.write(key, "seed")  # also asserts the preference list is the three nodes
    replicas = cluster.handles_for(key)
    require(len(replicas) == 3, f"expected 3 replicas for {key}, got {len(replicas)}")

    lock = threading.Lock()
    acked: list[dict[str, Any]] = []

    def writer(index: int) -> None:
        response = cluster.put(key, f"val-{index}")
        if response.status_code == 200:
            with lock:
                acked.append(response.json())

    writers = [threading.Thread(target=writer, args=(i,)) for i in range(12)]
    for thread in writers:
        thread.start()
    for thread in writers:
        thread.join()

    assert len(acked) == 12, f"only {len(acked)} of 12 concurrent writes met quorum"

    # The coordinator assigns a unique, monotonically increasing version per write, so there is
    # one unambiguous winner: the highest version returned. Last-writer-wins means every replica
    # must settle on exactly that version once the background pushes land.
    winner = max(acked, key=lambda body: _version_tuple(body["version"]))
    winning_version = _version_tuple(winner["version"])

    def converged() -> bool:
        entries = [cluster.node_entry(handle, key) for handle in replicas]
        return all(entry is not None and entry[1] == winning_version for entry in entries)

    wait_until(converged, 10, "all replicas to converge on the winning write")

    final = [cluster.node_entry(handle, key) for handle in replicas]
    values = {entry[0] for entry in final if entry is not None}
    assert len(values) == 1, f"replicas disagree on the value: {values}"

    # A coordinator read returns that same converged value.
    response = cluster.get(key)
    assert response.status_code == 200, f"GET returned {response.status_code}: {response.text}"
    assert response.json()["value"] in values
    assert _version_tuple(response.json()["version"]) == winning_version
