"""Orchestrate a consistency run: workload + nemesis, then heal, scan, and check.

Runnable as a module for local long runs and the R=1/W=1 failure demo::

    python -m tests.integration.checker.runner --duration 120 --read-quorum 2 --write-quorum 2
    python -m tests.integration.checker.runner --duration 60  --read-quorum 1 --write-quorum 1
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import httpx

# Allow `python -m tests.integration.checker.runner` and direct execution alike.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from tests.integration.checker.checks import CheckReport, run_all  # noqa: E402
from tests.integration.checker.history import (  # noqa: E402
    GET,
    NOT_FOUND,
    OK,
    History,
    Operation,
    ReplicaEntry,
)
from tests.integration.checker.workload import (  # noqa: E402
    ClientWorker,
    Nemesis,
    Recorder,
    _parse_version,
)
from tests.integration.harness import Cluster, NodeHandle  # noqa: E402


@dataclass
class CheckerConfig:
    nodes: int = 3
    replication_factor: int = 3
    read_quorum: int = 2
    write_quorum: int = 2
    duration: float = 20.0
    workers: int = 6
    key_count: int = 5
    seed: int = 1234
    # Read-repair is eventual; drive it over several post-heal read passes so convergence is a
    # reliable gate rather than a timing lottery. A system that can converge usually does in 1-3.
    converge_passes: int = 15
    # Long so a killed node stays a ring member for the whole run: membership never changes, so
    # the only forces at play are quorum, replication and read repair — not rebalance.
    dead_timeout: float = 600.0


@dataclass
class CheckerRun:
    history: History
    report: CheckReport
    op_count: int
    nemesis_actions: int
    status_counts: dict[int, int]
    converge_passes: int
    converged: bool


def _coordinator_env(cfg: CheckerConfig) -> dict[str, str]:
    return {
        "READ_QUORUM": str(cfg.read_quorum),
        "WRITE_QUORUM": str(cfg.write_quorum),
        "READ_REPAIR_ENABLED": "true",
    }


def _wait_read_repairs_quiesce(cluster: Cluster, timeout: float = 10.0) -> None:
    """Wait until the read-repair counter stops moving, so the final scan sees a settled cluster."""
    deadline = time.monotonic() + timeout
    last = cluster.metric("distrocache_read_repairs_total")
    stable_since = time.monotonic()
    while time.monotonic() < deadline:
        time.sleep(0.2)
        current = cluster.metric("distrocache_read_repairs_total")
        if current != last:
            last, stable_since = current, time.monotonic()
        elif time.monotonic() - stable_since >= 1.0:
            return


def _final_read_pass(url: str, keys: list[str], recorder: Recorder) -> None:
    """Read every key once through the coordinator (triggers read repair), recording each op."""
    with httpx.Client(timeout=10.0, limits=httpx.Limits(max_keepalive_connections=0)) as http:
        for key in keys:
            # perf_counter, not monotonic: on Windows monotonic has ~15 ms resolution, which ties
            # a read's invoke to a concurrent write's completion and fabricates happens-before.
            invoke = time.perf_counter()
            try:
                r = http.get(f"{url}/kv/{key}")
            except httpx.HTTPError:
                continue
            if r.status_code == 200:
                body = r.json()
                recorder.add(
                    Operation(
                        GET,
                        key,
                        invoke,
                        time.perf_counter(),
                        OK,
                        value=body.get("value"),
                        version=_parse_version(body),
                        status=200,
                        worker=-1,
                    )
                )
            elif r.status_code == 404:
                recorder.add(
                    Operation(
                        GET, key, invoke, time.perf_counter(), NOT_FOUND, status=404, worker=-1
                    )
                )


def _scan_replicas(cluster: Cluster, keys: list[str]) -> dict[str, list[ReplicaEntry]]:
    scan: dict[str, list[ReplicaEntry]] = {}
    for key in keys:
        entries: list[ReplicaEntry] = []
        for handle in cluster.nodes.values():
            result = cluster.node_scan(handle, key)
            if result is not None:
                version, tombstone = result
                entries.append(
                    ReplicaEntry(node=handle.node_id, version=version, tombstone=tombstone)
                )
        if entries:
            scan[key] = entries
    return scan


def _all_converged(scan: dict[str, list[ReplicaEntry]]) -> bool:
    for entries in scan.values():
        if len({(e.version, e.tombstone) for e in entries}) > 1:
            return False
    return True


def _drive_convergence(
    cluster: Cluster, keys: list[str], recorder: Recorder, max_passes: int
) -> tuple[dict[str, list[ReplicaEntry]], int, bool]:
    """Read every key repeatedly to drive read-repair until replicas agree, or passes run out.

    Convergence is *eventual*: it is restored by read repair, which is best-effort and skips a
    replica whose vote errors on a channel still warming up after a restart. A single pass can
    therefore leave a just-healed replica stale. Repeating the read pass warms the channel and
    retries the repair, so a system that can converge does — generously bounded so a system that
    cannot (e.g. a genuine bug) still terminates and is reported as a divergence violation.
    """
    scan: dict[str, list[ReplicaEntry]] = {}
    for used in range(1, max_passes + 1):
        _final_read_pass(cluster.url, keys, recorder)
        _wait_read_repairs_quiesce(cluster)
        scan = _scan_replicas(cluster, keys)
        if _all_converged(scan):
            return scan, used, True
    return scan, max_passes, _all_converged(scan)


def run_checker(cfg: CheckerConfig) -> CheckerRun:
    """Run one full consistency check and return the history, scan and report.

    Builds its own cluster, so callers pass only configuration. The cluster is always torn down.
    """
    cluster = Cluster(
        Path(_make_workdir()),
        replication_factor=cfg.replication_factor,
        dead_timeout=cfg.dead_timeout,
        coordinator_env=_coordinator_env(cfg),
    )
    recorder = Recorder()
    keys = [f"key-{i}" for i in range(cfg.key_count)]
    stop = threading.Event()
    try:
        cluster.start(cfg.nodes)
        workers = [
            ClientWorker(i, cluster.url, keys, recorder, stop, seed=cfg.seed + i)
            for i in range(cfg.workers)
        ]
        nemesis = Nemesis(
            cluster, stop, max_down=cfg.replication_factor - cfg.write_quorum, seed=cfg.seed
        )
        for w in workers:
            w.start()
        nemesis.start()

        time.sleep(cfg.duration)

        stop.set()
        nemesis.join(timeout=90)
        for w in workers:
            w.join(timeout=20)

        # Authoritative, quiet heal: with clients stopped, clean-restart the nodes that are
        # behind (down or stale) so each one's coordinator gRPC channel is confirmed reconnected
        # before convergence is judged. Only behind nodes need it (see _heal_behind), which keeps
        # the heal — the slowest part on a loaded CI runner — proportional to the damage.
        _heal_behind(cluster, keys)
        scan, passes, converged = _drive_convergence(cluster, keys, recorder, cfg.converge_passes)

        history = History(ops=list(recorder.ops), final_scan=scan)
        report = run_all(history)
        status_counts: dict[int, int] = {}
        for op in history.ops:
            if op.status is not None:
                status_counts[op.status] = status_counts.get(op.status, 0) + 1
        return CheckerRun(
            history, report, len(history.ops), nemesis.actions, status_counts, passes, converged
        )
    finally:
        stop.set()
        cluster.close(print_logs=False)


def _behind_nodes(cluster: Cluster, keys: list[str]) -> list[NodeHandle]:
    """Nodes that are down or hold an older version than the newest seen for some key.

    With N=3 on 3 nodes every node owns every key, so a node missing a key or holding an older
    version than another node is genuinely behind. A current node needs no repair, so it is left
    alone — only behind nodes are restarted during the heal.
    """
    # (handle, scan) pairs — NodeHandle is not hashable, so do not use it as a dict key.
    scans: list[tuple[NodeHandle, dict[str, tuple[tuple[int, int, str], bool]] | None]] = []
    for handle in cluster.nodes.values():
        try:
            scan = {k: v for k in keys if (v := cluster.node_scan(handle, k)) is not None}
        except Exception:
            scan = None  # unreachable right now
        scans.append((handle, scan))
    best: dict[str, tuple[int, int, str]] = {}
    for key in keys:
        versions = [s[key][0] for _, s in scans if s and key in s]
        if versions:
            best[key] = max(versions)
    behind = []
    for handle, scan in scans:
        if scan is None or any(
            key in best and (key not in scan or scan[key][0] < best[key]) for key in keys
        ):
            behind.append(handle)
    return behind


def _heal_behind(cluster: Cluster, keys: list[str]) -> None:
    """Clean-restart the behind nodes (quietly, clients stopped) so convergence can be driven.

    A node relaunched mid-run can be *serving* yet unreachable over the coordinator's cached gRPC
    channel; its read vote then errors and read repair skips it, so a stale replica would never
    converge. ``cluster.restart`` waits for the coordinator to reach the node again, confirming the
    channel. Data persists on disk across the restart, so nothing is lost; restarts run one at a
    time, so a quorum always remains. Restarting only the behind nodes (usually one) keeps this —
    the slowest step on a loaded runner — from dominating the run. Checking /health is not enough:
    it can report a cold channel as healthy.
    """
    for handle in _behind_nodes(cluster, keys):
        for _ in range(4):
            try:
                if handle.process.running:
                    cluster.kill(handle)
                cluster.restart(handle)
                break
            except Exception:
                time.sleep(1.0)


def _make_workdir() -> str:
    import tempfile

    return tempfile.mkdtemp(prefix="distrocache-checker-")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="DistroCache consistency checker")
    parser.add_argument("--duration", type=float, default=20.0)
    parser.add_argument("--read-quorum", type=int, default=2)
    parser.add_argument("--write-quorum", type=int, default=2)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--keys", type=int, default=5)
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--converge-passes", type=int, default=15)
    args = parser.parse_args(argv)
    cfg = CheckerConfig(
        nodes=args.nodes,
        replication_factor=args.replication_factor,
        read_quorum=args.read_quorum,
        write_quorum=args.write_quorum,
        duration=args.duration,
        workers=args.workers,
        key_count=args.keys,
        seed=args.seed,
        converge_passes=args.converge_passes,
    )
    n, r, w = cfg.replication_factor, cfg.read_quorum, cfg.write_quorum
    print(
        f"Running checker: N={n} R={r} W={w} duration={cfg.duration}s "
        f"(R+W{'>' if r + w > n else '<='}N)"
    )
    run = run_checker(cfg)
    print(
        f"\nOperations: {run.op_count}  nemesis actions: {run.nemesis_actions}  "
        f"statuses: {dict(sorted(run.status_counts.items()))}"
    )
    print(
        f"Convergence: {'converged' if run.converged else 'DID NOT converge'} "
        f"after {run.converge_passes} read pass(es)"
    )
    print(run.report.summary())
    clean = run.report.ok
    print(f"\nREPORT: {'CLEAN' if clean else 'VIOLATIONS FOUND'}")
    if r + w > n:
        # Guarantees hold: a clean report is success.
        return 0 if clean else 1
    # R + W <= N: the guarantees do not hold, so violations are the expected, successful outcome.
    return 0 if not clean else 2


if __name__ == "__main__":
    raise SystemExit(main())
