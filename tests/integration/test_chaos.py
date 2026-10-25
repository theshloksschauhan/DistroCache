"""Killing a replica mid-traffic must never turn a request into a 500."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import pytest

from tests.integration.harness import Cluster, require

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]


def test_no_500s_when_one_of_three_replicas_is_killed_during_traffic(
    cluster_factory: ClusterFactory,
) -> None:
    # N=3 so every key lives on all three nodes; killing one still leaves R=W=2 reachable.
    # A long dead_timeout keeps the killed node a ring member, so the ring never changes and
    # the only thing under test is the request path skipping a down replica.
    cluster = cluster_factory(nodes=3, dead_timeout=60)
    keys = [f"hot-{i}" for i in range(5)]
    for key in keys:
        cluster.write(key, "v0")

    stop = threading.Event()
    lock = threading.Lock()
    statuses: list[int] = []
    client_errors: list[str] = []

    def hammer(worker: int) -> None:
        i = 0
        while not stop.is_set():
            key = keys[(worker + i) % len(keys)]
            try:
                if i % 2 == 0:
                    response = cluster.put(key, f"w{worker}-{i}")
                else:
                    response = cluster.get(key)
            except Exception as exc:  # a dropped connection is itself a failure to record
                with lock:
                    client_errors.append(repr(exc))
            else:
                with lock:
                    statuses.append(response.status_code)
            i += 1

    workers = [threading.Thread(target=hammer, args=(w,)) for w in range(4)]
    for thread in workers:
        thread.start()
    try:
        time.sleep(1.0)  # let traffic flow against the whole cluster first
        with lock:
            before_kill = len(statuses)
        cluster.kill(cluster.nodes["node2"])
        time.sleep(2.0)  # and keep hammering with one replica down
    finally:
        stop.set()
        for thread in workers:
            thread.join()

    require(before_kill > 0, "no requests completed before the kill")
    require(len(statuses) > before_kill, "no requests completed after the kill")
    assert not client_errors, f"requests raised client-side errors: {client_errors[:3]}"
    # The guarantee: a replica dying under load never crashes a request. 503 (below quorum) is a
    # clean refusal and allowed; any 5xx that is not 503 means the coordinator itself failed.
    server_errors = sorted({s for s in statuses if s >= 500 and s != 503})
    assert not server_errors, f"requests returned server errors: {server_errors}"
    assert set(statuses) <= {200, 503}, f"unexpected statuses: {sorted(set(statuses))}"

    # With two healthy replicas for every key, the cluster is still fully serving afterward.
    # Health detection is eventually consistent: right after the kill, a probe to a *healthy*
    # node can transiently time out on a slow runner, briefly dropping the cluster below quorum
    # (a clean 503, exactly as tolerated during the load above). Wait for detection to settle,
    # then require 200 -- if two healthy replicas genuinely could not serve, this still fails.
    for key in keys:
        deadline = time.monotonic() + 15.0
        response = cluster.get(key)
        while response.status_code != 200 and time.monotonic() < deadline:
            time.sleep(0.2)
            response = cluster.get(key)
        assert response.status_code == 200, (
            f"GET {key} never returned 200 within 15s after the kill: "
            f"{response.status_code}: {response.text}"
        )
