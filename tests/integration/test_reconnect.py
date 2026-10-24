"""A restarted node must accept writes again within a few seconds.

The node stays dead while the coordinator keeps calling it, so the channel
fails several reconnect attempts and the backoff grows. A fixed 20s pause is
not enough to tell the schedules apart: gRPC's next attempt can still fall
inside the 5s budget. The coordinator log says how long the next wait is.
The test relaunches when that wait exceeds 10s, which the default schedule
reaches and the 2s cap never does.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable

import pytest

from tests.integration.harness import Cluster, require, wait_until

pytestmark = pytest.mark.integration

ClusterFactory = Callable[..., Cluster]

# Default grpc 1.64 on Windows climbed through roughly 0.4s, 2.8s, 5.3s, 10s
# over about 20s, but the next attempt can still land inside a fixed 5s window.
# Relaunch only once a logged wait is longer than process start plus that window.
_LONG_BACKOFF_MS = 10_000
# The 2s cap stays under this. After MIN_DEAD_SECONDS, a channel that never
# crossed it is the capped one, and the 5s budget is enough.
_CAPPED_BACKOFF_MS = 2_500
_BACKOFF_LINE = re.compile(r"backing off for (-?\d+) ms")
MIN_DEAD_SECONDS = 20.0
HUNT_SECONDS = 45.0
WRITE_EVERY_SECONDS = 0.5
REACH_WITHIN_SECONDS = 5.0


def _backoff_delays_ms(log_text: str, address: str) -> list[int]:
    """Positive reconnect waits logged for this address, in order.

    Early lines can be negative; gRPC then retries immediately. Those are not
    a wait the test can sit behind.
    """
    delays: list[int] = []
    for line in log_text.splitlines():
        if f"address=ipv4:{address}," not in line:
            continue
        match = _BACKOFF_LINE.search(line)
        if match is None:
            continue
        delay = int(match.group(1))
        if delay > 0:
            delays.append(delay)
    return delays


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="gRPC's default reconnect backoff grows past 5s while a node is down",
)
def test_write_reaches_a_restarted_node_within_a_few_seconds(
    cluster_factory: ClusterFactory,
) -> None:
    # gRPC logs "backing off for N ms" on the coordinator only. The line is how
    # the test tells a 2s cap from the default schedule.
    cluster = cluster_factory(
        nodes=3,
        heartbeat_timeout=60,
        coordinator_env={"GRPC_TRACE": "subchannel", "GRPC_VERBOSITY": "INFO"},
    )
    _run(cluster)


def _run(cluster: Cluster) -> None:
    target = cluster.nodes["node2"]
    key = cluster.key_owned_first_by(target, prefix="re")
    cluster.write(key, "before")  # opens the coordinator's channel to this node

    cluster.kill(target)
    dead_started = time.monotonic()
    hunt_until = dead_started + HUNT_SECONDS
    long_backoff_ms: int | None = None
    while time.monotonic() < hunt_until:
        cluster.put(key, "while-down")
        delays = _backoff_delays_ms(cluster.coordinator.log_text(), target.address)
        long = [delay for delay in delays if delay >= _LONG_BACKOFF_MS]
        if long:
            # The line is written when the wait starts, so the full delay remains.
            long_backoff_ms = long[-1]
            break
        elapsed = time.monotonic() - dead_started
        if elapsed >= MIN_DEAD_SECONDS and delays and max(delays) <= _CAPPED_BACKOFF_MS:
            break
        remaining = hunt_until - time.monotonic()
        if remaining > 0:
            time.sleep(min(WRITE_EVERY_SECONDS, remaining))

    delays = _backoff_delays_ms(cluster.coordinator.log_text(), target.address)
    dead_s = time.monotonic() - dead_started
    require(delays, "coordinator log had no reconnect backoff lines")

    def coordinator_sees_it_down() -> bool:
        state = cluster.http.get(f"{cluster.url}/health").json()["nodes"].get(target.address)
        return bool(state and not state["ok"])

    # A long wait has just started. Do not spend it blocking on this check.
    if long_backoff_ms is None:
        wait_until(coordinator_sees_it_down, 10, "the coordinator to notice the killed node")
    else:
        require(
            coordinator_sees_it_down(),
            "the coordinator had not noticed the killed node when backoff grew",
        )
    # The node is serving again. Do not wait for the coordinator's channel.
    relaunch_started = time.monotonic()
    cluster.relaunch(target)
    relaunch_s = time.monotonic() - relaunch_started

    deadline = time.monotonic() + REACH_WITHIN_SECONDS
    stored = False
    while time.monotonic() < deadline:
        cluster.put(key, "after")
        try:
            entry = cluster.node_entry(target, key)
        except Exception:
            entry = None
        if entry is not None and entry[0] == "after":
            stored = True
            break
        time.sleep(0.25)
    reach_s = REACH_WITHIN_SECONDS - max(0.0, deadline - time.monotonic())

    assert stored, (
        f"{target.node_id} restarted on {target.address} but held "
        f"{cluster.node_entry(target, key)} after {dead_s:.1f}s down "
        f"and {REACH_WITHIN_SECONDS:.0f}s of writes "
        f"(long backoff {long_backoff_ms} ms, max {max(delays)} ms, "
        f"relaunch {relaunch_s:.2f}s, reach {reach_s:.2f}s)"
    )
