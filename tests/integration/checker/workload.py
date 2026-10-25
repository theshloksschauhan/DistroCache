"""Concurrent client workers and the node-killing nemesis.

Workers drive random get/put/delete over a small key set and record each operation into a
shared :class:`History`. The nemesis kills and restarts random nodes, never taking down more
than ``N - W`` at once, so a healthy quorum always remains reachable.
"""

from __future__ import annotations

import random
import threading
import time

import httpx

from tests.integration.harness import Cluster, NodeHandle

from .history import DELETE, ERROR, FAIL, GET, NOT_FOUND, OK, PUT, Operation, Version


def _parse_version(body: dict) -> Version | None:
    v = body.get("version")
    if not isinstance(v, dict):
        return None
    return (int(v["physical_ms"]), int(v["logical"]), str(v["coordinator_id"]))


class Recorder:
    """A thread-safe sink for operations."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.ops: list[Operation] = []

    def add(self, op: Operation) -> None:
        with self._lock:
            self.ops.append(op)


class ClientWorker(threading.Thread):
    """Drives random operations against the coordinator until stopped."""

    def __init__(
        self,
        worker_id: int,
        url: str,
        keys: list[str],
        recorder: Recorder,
        stop: threading.Event,
        seed: int,
    ) -> None:
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.url = url
        self.keys = keys
        self.recorder = recorder
        self.stop = stop
        self.rng = random.Random(seed)
        self.seq = 0
        # Own client per worker: no keep-alive so a connection reset by a 5xx is never reused.
        self.http = httpx.Client(timeout=10.0, limits=httpx.Limits(max_keepalive_connections=0))

    def run(self) -> None:
        try:
            while not self.stop.is_set():
                self._one()
        finally:
            self.http.close()

    # Operation times use time.perf_counter(), not time.monotonic(): on Windows monotonic has
    # ~15 ms resolution, which ties a read's invoke to a concurrent write's completion and makes
    # the read-your-writes check read a false happens-before. perf_counter is sub-microsecond.
    def _one(self) -> None:
        key = self.rng.choice(self.keys)
        roll = self.rng.random()
        if roll < 0.5:
            self._op_get(key)
        elif roll < 0.9:
            self._op_put(key)
        else:
            self._op_delete(key)

    def _record(
        self,
        op: str,
        key: str,
        invoke: float,
        outcome: str,
        *,
        value: str | None = None,
        version: Version | None = None,
        status: int | None = None,
    ) -> None:
        self.recorder.add(
            Operation(
                op=op,
                key=key,
                invoke=invoke,
                complete=time.perf_counter(),
                outcome=outcome,
                value=value,
                version=version,
                status=status,
                worker=self.worker_id,
            )
        )

    def _classify_error(self, op: str, key: str, invoke: float, status: int) -> None:
        # 503 is a clean below-quorum refusal (unknown result); anything else >= 500 is a defect.
        self._record(op, key, invoke, FAIL if status == 503 else ERROR, status=status)

    def _op_get(self, key: str) -> None:
        invoke = time.perf_counter()
        try:
            r = self.http.get(f"{self.url}/kv/{key}")
        except httpx.HTTPError:
            self._record(GET, key, invoke, FAIL)
            return
        if r.status_code == 200:
            body = r.json()
            self._record(
                GET,
                key,
                invoke,
                OK,
                value=body.get("value"),
                version=_parse_version(body),
                status=200,
            )
        elif r.status_code == 404:
            self._record(GET, key, invoke, NOT_FOUND, status=404)
        else:
            self._classify_error(GET, key, invoke, r.status_code)

    def _op_put(self, key: str) -> None:
        self.seq += 1
        value = f"w{self.worker_id}-{self.seq}"  # unique per (worker, seq) so reads are traceable
        invoke = time.perf_counter()
        try:
            r = self.http.post(f"{self.url}/kv/{key}", json={"value": value})
        except httpx.HTTPError:
            # The write may or may not have landed; record the attempted value either way so a
            # later read of it is not treated as fabricated.
            self._record(PUT, key, invoke, FAIL, value=value)
            return
        if r.status_code == 200:
            self._record(
                PUT, key, invoke, OK, value=value, version=_parse_version(r.json()), status=200
            )
        else:
            self._record(
                PUT,
                key,
                invoke,
                FAIL if r.status_code == 503 else ERROR,
                value=value,
                status=r.status_code,
            )

    def _op_delete(self, key: str) -> None:
        invoke = time.perf_counter()
        try:
            r = self.http.delete(f"{self.url}/kv/{key}")
        except httpx.HTTPError:
            self._record(DELETE, key, invoke, FAIL)
            return
        if r.status_code == 200:
            self._record(DELETE, key, invoke, OK, version=_parse_version(r.json()), status=200)
        else:
            self._classify_error(DELETE, key, invoke, r.status_code)


class Nemesis(threading.Thread):
    """Kills and restarts random nodes, keeping at most ``max_down`` down at once."""

    def __init__(
        self,
        cluster: Cluster,
        stop: threading.Event,
        max_down: int,
        seed: int,
        period: float = 0.8,
    ) -> None:
        super().__init__(daemon=True)
        self.cluster = cluster
        self.stop = stop
        self.max_down = max_down
        self.rng = random.Random(seed)
        self.period = period
        self.down: list[NodeHandle] = []
        self.actions = 0

    def run(self) -> None:
        if self.max_down < 1:
            return  # W == N: taking any node down would break every quorum; nothing safe to do
        while not self.stop.is_set():
            time.sleep(self.period)
            try:
                self._act()
            except Exception:
                # A kill/relaunch race (e.g. a port still releasing) must not end the nemesis; the
                # harness raises HarnessError, which is not a consistency violation.
                pass
        self._heal()

    def _bring_up(self, handle: NodeHandle) -> None:
        """Start a killed node and wait for it to serve — but not for the coordinator to reach it.

        ``cluster.relaunch`` (unlike ``restart``) does not block on the coordinator's gRPC channel
        reconnecting. That wait can take tens of seconds under concurrent load, which would stall
        the nemesis and leave the node half-started; the coordinator reconnects on its own as its
        health loop and the clients' traffic drive RPCs to the node again.
        """
        self.cluster.relaunch(handle)

    def _act(self) -> None:
        up = [h for h in self.cluster.nodes.values() if h not in self.down]
        # Restart a down node, or take a fresh one down, biased toward keeping churn going.
        if self.down and (len(self.down) >= self.max_down or self.rng.random() < 0.5):
            handle = self.rng.choice(self.down)
            self._bring_up(handle)
            self.down.remove(handle)
            self.actions += 1
        elif up and len(self.down) < self.max_down:
            handle = self.rng.choice(up)
            self.cluster.kill(handle)
            self.down.append(handle)
            self.actions += 1

    def _heal(self) -> None:
        # Best-effort: get any node it left down serving again. The runner does the authoritative
        # quiet heal afterwards (clean-restarting any node the coordinator still can't reach), so
        # this only needs to start the process, not confirm the coordinator's channel.
        for handle in list(self.down):
            try:
                self._bring_up(handle)
                self.down.remove(handle)
            except Exception:
                pass
