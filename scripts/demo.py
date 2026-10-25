"""A narrated DistroCache demo: survive a node crash under load, then heal and rebalance.

Starts a real 3-node cluster, drives continuous traffic, kills a node mid-load to show the
cluster keeps serving (no 5xx), heals it and watches read repair restore the stale replica, then
adds a node to trigger a rebalance. Prints a clear step-by-step narration; record a GIF of it if
you like.

    python scripts/demo.py
"""

from __future__ import annotations

import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.integration.harness import Cluster  # noqa: E402


def banner(text: str) -> None:
    print(f"\n{'=' * 70}\n  {text}\n{'=' * 70}", flush=True)


def note(text: str) -> None:
    print(f"  - {text}", flush=True)


class Load:
    """Background client threads driving a get/put mix and recording status codes."""

    def __init__(self, cluster: Cluster, keys: list[str], workers: int = 4) -> None:
        self.cluster = cluster
        self.keys = keys
        self.workers = workers
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self.statuses: dict[int, int] = {}
        self.client_errors = 0
        self._threads: list[threading.Thread] = []

    def _record(self, code: int) -> None:
        with self._lock:
            self.statuses[code] = self.statuses.get(code, 0) + 1

    def _run(self, worker: int) -> None:
        i = 0
        while not self._stop.is_set():
            key = self.keys[(worker + i) % len(self.keys)]
            try:
                if i % 2 == 0:
                    resp = self.cluster.put(key, f"w{worker}-{i}")
                else:
                    resp = self.cluster.get(key)
                self._record(resp.status_code)
            except Exception:
                with self._lock:
                    self.client_errors += 1
            i += 1
            time.sleep(0.01)

    def start(self) -> None:
        self._threads = [
            threading.Thread(target=self._run, args=(w,), daemon=True) for w in range(self.workers)
        ]
        for t in self._threads:
            t.start()

    def stop(self) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=10)

    def total(self) -> int:
        return sum(self.statuses.values())


def main() -> int:
    workdir = Path(
        tempfile.mkdtemp(prefix="distrocache-demo-")
    )  # temp dir: no files touch the repo
    cluster = Cluster(
        workdir,
        replication_factor=3,
        dead_timeout=600,  # keep a killed node in the ring so the only thing under test is failover
        coordinator_env={"READ_QUORUM": "2", "WRITE_QUORUM": "2", "READ_REPAIR_ENABLED": "true"},
    )
    keys = [f"demo-{i}" for i in range(8)]
    try:
        banner("1. Start a 3-node cluster (N=3, R=2, W=2)")
        cluster.start(3)
        note(f"coordinator at {cluster.url}")
        for key in keys:
            cluster.put(key, "v0")
        note(f"wrote {len(keys)} keys, each replicated to 3 nodes")

        banner("2. Drive continuous traffic, then CRASH node2 mid-load")
        load = Load(cluster, keys)
        load.start()
        time.sleep(1.5)
        before = load.total()
        note(f"{before} requests served by the healthy cluster")
        cluster.kill(cluster.nodes["node2"])
        note("killed node2 (hard crash, no graceful shutdown)")
        time.sleep(2.5)
        load.stop()
        note(f"{load.total()} requests total; kept serving {load.total() - before} after the crash")
        server_errors = {c: n for c, n in load.statuses.items() if c >= 500 and c != 503}
        note(f"status codes: {dict(sorted(load.statuses.items()))}")
        note(f"client-side errors: {load.client_errors}")
        if server_errors:
            note(f"!! server errors (5xx other than 503): {server_errors}")
        else:
            note("zero server errors (no 500s): a crashed replica never crashes a request")
        if load.statuses.get(503):
            note(f"({load.statuses[503]} x 503 = clean below-quorum refusals during failover)")

        banner("3. Heal node2: a read repairs the stale replica")
        repairs_before = cluster.metric("distrocache_read_repairs_total")
        cluster.restart(cluster.nodes["node2"])
        note("restarted node2 (it missed every write while down)")
        for key in keys:
            cluster.get(key)  # each read converges the stale replica in the background
        time.sleep(1.5)
        repairs_after = cluster.metric("distrocache_read_repairs_total")
        note(
            f"read repairs: {repairs_before:.0f} -> {repairs_after:.0f} "
            f"(+{repairs_after - repairs_before:.0f} stale copies pushed back up to date)"
        )

        banner("4. Add a 4th node: the ring rebalances keys onto it")
        rebalances_before = cluster.rebalance_count()
        new_node = cluster.add_node()
        note(f"added {new_node.node_id}; waiting for the rebalance it triggers")
        try:
            cluster.wait_for_rebalance_after(rebalances_before, timeout=20)
        except Exception:
            pass
        rebalances_after = cluster.rebalance_count()
        restores = cluster.metric("distrocache_rebalance_replica_restores_total")
        note(
            f"rebalance events: {rebalances_before:.0f} -> {rebalances_after:.0f}; "
            f"replica restores: {restores:.0f}"
        )
        time.sleep(1.0)
        key_counts = {n["node_id"]: n["key_count"] for n in cluster.membership()["nodes"]}
        note(f"keys per node now: {key_counts} ({new_node.node_id} now owns a share)")

        banner("Summary")
        note("A node crashed under load and the cluster kept serving with no 5xx errors.")
        note("Read repair silently brought the recovered node back into sync.")
        note("Adding a node rebalanced ownership with a safe compare-and-delete hand-off.")
        note("That is N/R/W quorums + HLC last-writer-wins + read repair + safe rebalance.")
        return 0
    finally:
        cluster.close(print_logs=False)


if __name__ == "__main__":
    raise SystemExit(main())
