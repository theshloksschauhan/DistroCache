"""Run a real coordinator and real storage nodes as subprocesses on localhost.

Everything is isolated per test: free ports, a temporary directory for SQLite files and
logs, and processes that are always killed at teardown. Works on Linux and Windows.

Errors raised by the harness itself are HarnessError (a RuntimeError, never an
AssertionError), so a bug-reproduction test marked xfail(raises=AssertionError) cannot
pass by accident when the cluster failed to start or a precondition did not hold.
"""

from __future__ import annotations

import atexit
import os
import re
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

import grpc
import httpx

from distrocache_common.generated import cache_pb2, cache_pb2_grpc
from distrocache_common.hashing import ConsistentHashRing

REPO_ROOT = Path(__file__).resolve().parents[2]
IS_WINDOWS = sys.platform == "win32"
STARTUP_TIMEOUT = 30.0

# Fast loops so tests don't wait on production intervals.
NODE_HEARTBEAT_INTERVAL = 0.2
COORDINATOR_PRUNE_INTERVAL = 0.2

_live_processes: set[subprocess.Popen[bytes]] = set()


@atexit.register
def _kill_leftovers() -> None:
    # Last resort if a test run is interrupted before fixture teardown.
    for popen in list(_live_processes):
        if popen.poll() is None:
            popen.kill()


class HarnessError(RuntimeError):
    """The harness or the cluster misbehaved; not the behaviour under test."""


def require(condition: bool, message: str) -> None:
    """Check a precondition. Failing raises HarnessError, never AssertionError."""
    if not condition:
        raise HarnessError(message)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_until(
    predicate: Callable[[], bool], timeout: float, description: str, interval: float = 0.1
) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except HarnessError:
            raise  # e.g. the process died: waiting longer can't help
        except Exception as exc:  # the cluster may still be starting
            last_error = exc
        time.sleep(interval)
    raise HarnessError(
        f"timed out after {timeout:.0f}s waiting for {description} (last error: {last_error!r})"
    )


_SAMPLE = re.compile(r"^(?P<name>[A-Za-z_:][\w:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>\S+)")
_LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def parse_metric(text: str, name: str, **labels: str) -> float:
    """Sum every sample of `name` whose labels include `labels` (0.0 if none)."""
    total = 0.0
    for line in text.splitlines():
        match = _SAMPLE.match(line)
        if not match or match["name"] != name:
            continue
        sample_labels = dict(_LABEL.findall(match["labels"] or ""))
        if all(sample_labels.get(k) == v for k, v in labels.items()):
            total += float(match["value"])
    return total


class Process:
    """One child process whose output goes to a log file."""

    def __init__(self, name: str, args: list[str], env: dict[str, str], log_path: Path) -> None:
        self.name = name
        self.args = args
        self.env = env
        self.log_path = log_path
        self.popen: subprocess.Popen[bytes] | None = None
        self._log: IO[bytes] | None = None

    @property
    def running(self) -> bool:
        return self.popen is not None and self.popen.poll() is None

    def start(self) -> None:
        require(not self.running, f"{self.name} is already running")
        self._log = open(self.log_path, "ab")  # closed in _reap()
        self._log.write(f"\n===== start {self.name} at {time.strftime('%X')} =====\n".encode())
        self._log.flush()
        # Own process group on Windows so CTRL_BREAK reaches only this child.
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if IS_WINDOWS else 0
        self.popen = subprocess.Popen(
            self.args,
            cwd=REPO_ROOT,
            env=self.env,
            stdin=subprocess.DEVNULL,
            stdout=self._log,
            stderr=subprocess.STDOUT,
            creationflags=flags,
        )
        _live_processes.add(self.popen)

    def kill(self) -> None:
        """Hard kill: no shutdown code runs, like a crash or power loss."""
        if self.popen is not None and self.running:
            self.popen.kill()
        self._reap()

    def stop(self, timeout: float = 15.0) -> int:
        """Graceful stop (SIGTERM / CTRL_BREAK). Returns the exit code."""
        popen = self.popen
        if popen is None or popen.poll() is not None:
            raise HarnessError(f"{self.name} is not running")
        if IS_WINDOWS:
            popen.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            popen.terminate()
        try:
            code = popen.wait(timeout)
        except subprocess.TimeoutExpired:
            self.kill()
            raise HarnessError(f"{self.name} ignored a graceful stop for {timeout}s") from None
        self._reap()
        return code

    def _reap(self) -> None:
        if self.popen is not None:
            try:
                self.popen.wait(15)
            except subprocess.TimeoutExpired as exc:
                raise HarnessError(f"{self.name} did not exit after kill") from exc
            _live_processes.discard(self.popen)
        if self._log is not None:
            self._log.close()
            self._log = None

    def log_text(self) -> str:
        if not self.log_path.exists():
            return ""
        return self.log_path.read_text(encoding="utf-8", errors="replace")


@dataclass
class NodeHandle:
    node_id: str
    port: int
    storage_path: Path
    process: Process

    @property
    def address(self) -> str:
        return f"127.0.0.1:{self.port}"


class Cluster:
    """A coordinator plus N nodes. Build it with the `cluster_factory` fixture."""

    def __init__(
        self,
        workdir: Path,
        replication_factor: int = 3,
        heartbeat_timeout: float = 30.0,
        seed_nodes: bool = False,
    ) -> None:
        self.workdir = workdir
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.replication_factor = replication_factor
        self.heartbeat_timeout = heartbeat_timeout
        self.seed_nodes = seed_nodes
        self.nodes: dict[str, NodeHandle] = {}
        self.coordinator: Process | None = None
        self.url = ""
        self.http = httpx.Client(timeout=15.0)

    # ----- lifecycle -------------------------------------------------------------

    def start(self, node_count: int) -> None:
        # Ports are picked before the coordinator starts so they can be passed as seeds.
        handles = [self._new_node_handle() for _ in range(node_count)]
        self._start_coordinator([h.address for h in handles] if self.seed_nodes else [])
        for handle in handles:
            self._launch(handle)
        for handle in handles:
            self._wait_node_ready(handle)
        self.wait_for_rebalances_settled()

    def close(self, print_logs: bool) -> None:
        for handle in self.nodes.values():
            handle.process.kill()
        if self.coordinator is not None:
            # Hard kill on purpose: uvicorn's handling of CTRL_BREAK on Windows is not
            # something these tests should depend on.
            self.coordinator.kill()
        self.http.close()
        if print_logs:
            for process in self._processes():
                print(f"\n----- {process.name} log ({process.log_path}) -----")
                print(process.log_text()[-20_000:])

    def _processes(self) -> list[Process]:
        procs = [self.coordinator] if self.coordinator else []
        return procs + [h.process for h in self.nodes.values()]

    def _base_env(self) -> dict[str, str]:
        env = {k: v for k, v in os.environ.items() if k != "STORAGE_PATH"}
        env.update(
            PYTHONPATH=str(REPO_ROOT),
            PYTHONUNBUFFERED="1",
            PYTHONDONTWRITEBYTECODE="1",
            LOG_LEVEL="INFO",
        )
        return env

    def _start_coordinator(self, seeds: list[str]) -> None:
        port = free_port()
        env = self._base_env()
        env.update(
            NODE_ADDRESSES=",".join(seeds),
            REPLICATION_FACTOR=str(self.replication_factor),
            HEALTH_INTERVAL="0.5",
            HEARTBEAT_INTERVAL=str(COORDINATOR_PRUNE_INTERVAL),
            HEARTBEAT_TIMEOUT=str(self.heartbeat_timeout),
            REQUEST_TIMEOUT="0.5",
            RETRY_ATTEMPTS="2",
            RETRY_BACKOFF="0.05",
        )
        args = [sys.executable, "-m", "uvicorn", "backend.app:app"]
        args += ["--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"]
        self.coordinator = Process("coordinator", args, env, self.workdir / "coordinator.log")
        self.url = f"http://127.0.0.1:{port}"
        self.coordinator.start()

        def ready() -> bool:
            require(self.coordinator is not None and self.coordinator.running, "coordinator died")
            return self.http.get(f"{self.url}/membership/nodes").status_code == 200

        wait_until(ready, STARTUP_TIMEOUT, "the coordinator to accept requests")

    def _new_node_handle(self) -> NodeHandle:
        index = len(self.nodes) + 1
        node_id = f"node{index}"
        port = free_port()
        storage_path = self.workdir / f"{node_id}.db"
        env = self._base_env()
        env.update(
            NODE_ID=node_id,
            GRPC_HOST="127.0.0.1",
            GRPC_PORT=str(port),
            NODE_ADDRESS=f"127.0.0.1:{port}",
            HEARTBEAT_INTERVAL=str(NODE_HEARTBEAT_INTERVAL),
            REPLICATION_FACTOR=str(self.replication_factor),
            GRPC_TIMEOUT="0.5",
            STORAGE_PATH=str(storage_path),
        )
        process = Process(
            node_id, [sys.executable, "-m", "node.app"], env, self.workdir / f"{node_id}.log"
        )
        handle = NodeHandle(node_id, port, storage_path, process)
        self.nodes[node_id] = handle
        return handle

    def _launch(self, handle: NodeHandle) -> None:
        require(bool(self.url), "start the coordinator before any node")
        handle.process.env["BACKEND_URL"] = self.url
        handle.process.start()

    def _wait_node_ready(self, handle: NodeHandle) -> None:
        started = time.time()

        def ready() -> bool:
            require(handle.process.running, f"{handle.node_id} exited during startup")
            return self.node_health(handle) and self.node_status(handle) == "up"

        wait_until(ready, STARTUP_TIMEOUT, f"{handle.node_id} to serve and register")

        # The coordinator reuses one gRPC channel per address; after a restart that channel
        # may still be in reconnect backoff and fail requests. Its own health loop uses the
        # same channel, so a passing check made after the start means requests will get through.
        def coordinator_reaches_node() -> bool:
            state = self.http.get(f"{self.url}/health").json()["nodes"].get(handle.address)
            return bool(state and state["ok"] and state["last_seen"] > started)

        wait_until(
            coordinator_reaches_node, STARTUP_TIMEOUT, f"the coordinator to reach {handle.node_id}"
        )

    def add_node(self) -> NodeHandle:
        handle = self._new_node_handle()
        self._launch(handle)
        self._wait_node_ready(handle)
        return handle

    def kill(self, handle: NodeHandle) -> None:
        handle.process.kill()

    def stop(self, handle: NodeHandle) -> int:
        return handle.process.stop()

    def restart(self, handle: NodeHandle) -> None:
        """Start a stopped or killed node again: same id, port, address and database."""
        require(not handle.process.running, f"{handle.node_id} is still running")
        self._launch(handle)
        self._wait_node_ready(handle)

    # ----- coordinator views -------------------------------------------------------

    def get(self, key: str) -> httpx.Response:
        return self.http.get(f"{self.url}/kv/{key}")

    def put(self, key: str, value: str) -> httpx.Response:
        return self.http.post(f"{self.url}/kv/{key}", json={"value": value})

    def delete(self, key: str) -> httpx.Response:
        return self.http.delete(f"{self.url}/kv/{key}")

    def write(self, key: str, value: str) -> dict[str, Any]:
        """Setup write. Requires 200 and that the coordinator used our preference list."""
        expected = self.preference_list(key)
        response = self.put(key, value)
        require(response.status_code == 200, f"setup PUT {key} failed: {response.text}")
        body: dict[str, Any] = response.json()
        # If these differ, the harness would kill or inspect the wrong nodes.
        require(
            body.get("replicas") == expected,
            f"coordinator wrote {key} to {body.get('replicas')}, harness expected {expected}",
        )
        return body

    def membership(self) -> dict[str, Any]:
        response = self.http.get(f"{self.url}/membership/nodes")
        response.raise_for_status()
        data: dict[str, Any] = response.json()
        return data

    def node_status(self, handle: NodeHandle) -> str | None:
        """Status of the record registered under this node's id, or None if absent."""
        for record in self.membership()["nodes"]:
            if record["node_id"] == handle.node_id:
                return str(record["status"])
        return None

    def ring_version(self) -> int:
        return int(self.membership()["ring_version"])

    def preference_list(self, key: str) -> list[str]:
        """Replicas for `key` in coordinator order, from the coordinator's live membership."""
        live = [n["address"] for n in self.membership()["nodes"] if n["status"] == "up"]
        return ConsistentHashRing(live).get_nodes(key, self.replication_factor)

    def key_owned_first_by(self, handle: NodeHandle, prefix: str = "key") -> str:
        """A key whose preference list starts with `handle` (reads hit it first)."""
        for i in range(10_000):
            key = f"{prefix}-{i}"
            if self.preference_list(key)[0] == handle.address:
                return key
        raise HarnessError(f"no key found with {handle.node_id} as first replica")

    def handles_for(self, key: str) -> list[NodeHandle]:
        by_address = {h.address: h for h in self.nodes.values()}
        return [by_address[address] for address in self.preference_list(key)]

    def metric(self, name: str, **labels: str) -> float:
        response = self.http.get(f"{self.url}/metrics")
        response.raise_for_status()
        return parse_metric(response.text, name, **labels)

    def rebalance_count(self) -> float:
        return self.metric("distrocache_rebalance_events_total")

    def wait_for_rebalance_after(self, previous: float, timeout: float = 15.0) -> None:
        wait_until(lambda: self.rebalance_count() > previous, timeout, "a rebalance to be reported")

    def wait_for_rebalances_settled(self, quiet: float = 1.5, timeout: float = 30.0) -> None:
        """Wait until no rebalance has been reported for `quiet` seconds."""
        deadline = time.monotonic() + timeout
        last = self.rebalance_count()
        last_change = time.monotonic()
        while time.monotonic() < deadline:
            time.sleep(0.1)
            current = self.rebalance_count()
            if current != last:
                last, last_change = current, time.monotonic()
            elif time.monotonic() - last_change >= quiet:
                return
        raise HarnessError(f"rebalances did not settle within {timeout}s")

    # ----- direct node access -------------------------------------------------------

    def _call_node(self, handle: NodeHandle, method: str, request: Any) -> Any:
        with grpc.insecure_channel(handle.address) as channel:
            stub = cache_pb2_grpc.CacheNodeStub(channel)
            return getattr(stub, method)(request, timeout=2.0)

    def node_health(self, handle: NodeHandle) -> bool:
        response = self._call_node(handle, "Health", cache_pb2.HealthRequest())
        return bool(response.ok)

    def node_entry(self, handle: NodeHandle, key: str) -> tuple[str, int] | None:
        """(value, version) stored on that node, bypassing the coordinator."""
        response = self._call_node(handle, "GetEntry", cache_pb2.GetEntryRequest(key=key))
        if not response.found:
            return None
        return response.entry.value.decode("utf-8"), int(response.entry.version)
