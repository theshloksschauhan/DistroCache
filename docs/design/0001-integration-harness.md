# 0001: Integration harness and bug reproductions

Status: accepted (Phase 1, Stage 1)

## Context

Phase 1 turns DistroCache into a Dynamo-style store with guarantees that are checked, not
claimed. The audit found correctness bugs that only show up with several real processes:
dead replicas, stale replicas, missed deletes, rebalances. Unit tests could not show them, and
the modules could not be started in-process either: `node/app.py` opened its database at import
time and both apps kept their state in module globals with untracked background tasks.

Tests must run the same way on Windows (where the maintainer works, often without Docker) and
on Linux CI, and must never write `.db` files into the repository.

## Decision

### Make the processes startable and stoppable

- `node.app`: `load_config()` returns a `NodeConfig`; a `Node` owns storage, the membership
  client, the gRPC client pool and every background task (kept in a set, cancelled and awaited
  on shutdown). Nothing runs at import. `GRPC_HOST` sets the bind address (default `0.0.0.0`).
  On Windows, `CTRL_BREAK_EVENT` and Ctrl+C set the stop event via `signal.signal` and
  `call_soon_threadsafe`, since asyncio has no `add_signal_handler` there.
- `backend.app`: `create_app(settings)` puts a `Coordinator` on `app.state`; the lifespan starts
  its health and prune loops and cancels and awaits them on exit. `app = create_app()` remains
  for `uvicorn backend.app:app`.
- `Storage.count()` now holds the connection lock like every other query.

### A subprocess cluster harness

`tests/integration/harness.py` starts the coordinator (`uvicorn`) and nodes (`python -m
node.app`) with `sys.executable`, on free `127.0.0.1` ports, with SQLite files and logs in the
test's `tmp_path`. Children get their own process group on Windows so `CTRL_BREAK` reaches only
them. It provides:

- `kill()` (hard, no shutdown code), `stop()` (graceful; returns the exit code), `restart()`
  (same id, port, address and database), `add_node()`.
- Readiness: a node is ready when it answers gRPC `Health`, its record is `up`, and the
  coordinator's own health loop has reached it after the start. The last check matters because
  the coordinator caches one gRPC channel per address, and after a restart that channel can be in
  reconnect backoff and fail the next request.
- Views: membership records, `ring_version`, Prometheus metrics, and direct per-node
  `GetEntry` reads that bypass the coordinator.
- `preference_list(key)` computed from the coordinator's live membership. Every setup write goes
  through `write()`, which requires the PUT response's ordered `replicas` to equal it, so a test
  cannot kill or inspect the wrong node.
- Teardown always hard-kills every process, including the coordinator (uvicorn's handling of
  `CTRL_BREAK` is not something the tests rely on). Logs are printed only when a test really
  fails; an `atexit` hook kills leftovers if a run is interrupted.

Integration tests carry the `integration` marker and are excluded by default
(`addopts = -m "not integration"`). CI runs them on `ubuntu-24.04` and `windows-latest` with
Python 3.11. `pytest-timeout` bounds every test at 300 s.

### Bug reproductions as strict xfails

Each known bug has a test marked `xfail(strict=True, raises=AssertionError, reason=...)`, naming
the stage that fixes it. The fix commit removes the marker.

- `strict=True`: an unexpected pass fails the run, so a fixed bug can't keep its marker.
- `raises=AssertionError`: only the assertion on the bug itself counts as the expected failure.
  Harness problems and preconditions use `require()`, which raises `HarnessError` (a
  `RuntimeError`). A cluster that failed to start, a timeout, or a broken precondition is
  reported as a real failure.

| Test | Bug | Fixed in |
| --- | --- | --- |
| `test_failures::test_get_succeeds_when_first_replica_is_dead` | gRPC error aborts the GET loop with 500 | Stage 2 |
| `test_failures::test_failed_get_is_counted` | that 500 is not counted as a failure | Stage 2 |
| `test_versions::test_rebalance_does_not_replace_newer_value_with_older` | per-node counters let an older value win | Stage 3 |
| `test_quorum::test_write_without_quorum_is_rejected` | W=1 | Stage 4 |
| `test_quorum::test_stale_replica_does_not_hide_newer_value` | R=1, first hit wins | Stage 4 |
| `test_tombstones::test_delete_is_not_undone_by_a_replica_that_missed_it` | no tombstones | Stage 5 |
| `test_membership::test_seeded_nodes_have_one_live_record_each` | duplicate `seed-N` records | Stage 7 |
| `test_rebalance::test_rebalance_keeps_a_write_that_lands_during_the_push` (unit) | delete after snapshot loses a concurrent write | Stage 7 |

`test_join_leave` (200 keys, RF=2, one node joins, another leaves gracefully) passes on the
current code once rebalancing settles, so it is a regular regression test.

### What triggers a rebalance when a node restarts

Two tests depend on this, so it is spelled out here. Scenario: a node X is hard-killed and
restarted with the same `node_id` and address, while `HEARTBEAT_TIMEOUT` is long enough that
the coordinator never marked it down.

1. `MembershipRegistry.register` bumps `ring_version` only if there is no record for the id, the
   address changed, or the status is not `up`. None is true, so **no bump**. `heartbeat` bumps only
   for a new record or a down→up transition, so again no bump. (The node ignores the register
   response anyway.)
2. The node's `_heartbeat_loop` starts with `last_ring_version = -1`, so its **first successful
   heartbeat always** sees a different version and spawns `_rebalance` with the live addresses.
3. Other nodes rebalance only when the `ring_version` in their heartbeat response changes. It
   didn't, so **only X rebalances**, exactly once.
4. X's rebalance only pushes: for each local row it sends `PutReplica` with X's local version to
   the other owners, which apply it only if that version is higher than theirs. X never reads
   from peers, and deletes its own row only if X is no longer an owner.

Things that do bump the version and would make every node rebalance: a graceful `stop()`
(deregister, then a fresh register), a `HEARTBEAT_TIMEOUT` expiry (`prune_expired`), or a new
node. The version tests therefore use `kill()`, a 60 s timeout, and check with `require()` that
`ring_version` is unchanged.

- **Stale replica test** (`test_quorum`): X misses the second write, then restarts. The test
  waits for `distrocache_rebalance_events_total` to rise (X's startup rebalance has finished and
  reported), checks the ring version, and checks via direct reads that X still holds `old` and the
  others hold `new`. Then it reads. X's push can't repair X, and nothing else runs, so the read
  sees exactly the state the test set up.
- **Rebalance version test** (`test_versions`): X holds `("v3", 3)`; while X is down the key is
  deleted and rewritten as `v4`, so the others hold `("v4", 1)`. X's restart is what reliably
  triggers the rebalance under test. The test waits for the same metric rise and checks the
  ring version. Then it asserts that the others still hold `v4`.

Stage 7 changes membership and rebalancing. If a restart stops triggering a self-rebalance,
`wait_for_rebalance_after` will time out with a `HarnessError`. That is a loud failure, and
these two tests must then be updated, not silently pass.

## Consequences

- Correctness bugs are now executable: each later stage proves its fix by flipping an xfail, and
  regressions fail CI on Linux and Windows.
- The integration suite takes about two minutes per OS; each test starts its own cluster.
- Tests depend on a few internals: the rebalance metric as a completion signal, `GetEntry` for
  direct reads, and `/health` for coordinator reachability. Changes there must keep the harness
  working.
- Per-test clusters are slower than a shared one but keep tests independent; a failure in one
  cannot poison another.

## Alternatives considered

- **Docker Compose for integration tests.** Closest to production, but needs Docker, which is
  often not running on the maintainer's Windows machine, and is slow to iterate. Compose stays as
  the CI smoke test.
- **In-process cluster (TestClient + gRPC servers in one event loop).** Faster, but a hard
  kill can't be simulated and shared module state would leak between "nodes". The refactor
  makes it possible later for focused tests.
- **Plain `xfail` without `raises`.** Any exception, including a harness crash, would count as
  the expected failure and hide broken tests.
- **Sleeping a fixed time instead of waiting on the rebalance metric.** Simpler, but either
  flaky or slow, and it would not prove the rebalance actually happened.
