# 0008 — Consistency checker

Status: accepted

## Context

Batch 1 proved individual properties (quorum overlap, tombstones, read repair, safe rebalance)
with targeted integration tests. Those tests each set up one scenario. They do not answer the
harder question: **under concurrent traffic and random node failures, does the cluster as a whole
stay consistent?** A property that holds in isolation can still be violated by an interleaving no
single test thought to construct.

We want a checker that drives random operations against a real cluster while a nemesis kills and
restarts nodes, records a history, and afterwards verifies safety properties against that history
— in the spirit of Jepsen, but small and self-contained.

## Decision

`tests/integration/checker/` contains:

- **`history.py`** — the data model. An `Operation` records op kind, key, invoke/complete times
  (`time.monotonic`, so happens-before within the process is exact), outcome, value and version.
  A `History` holds the operations plus a `final_scan`: what every replica held for each key
  after the cluster healed, read directly from each node (bypassing the coordinator) so
  convergence and durability are judged against ground truth, not a quorum read.
- **`checks.py`** — five pure checks, history in, violations out. They are *observers*: they do
  not know N/R/W. See below.
- **`workload.py`** — concurrent `ClientWorker` threads (each its own HTTP client) driving random
  get/put/delete over a small key set, and a `Nemesis` thread that kills and restarts random
  nodes, never taking down more than `N - W` at once so a healthy quorum always remains.
- **`runner.py`** — orchestration: start a cluster, run workload + nemesis for a configurable
  duration, stop both, **heal**, **drive read repair to completion**, scan every replica, run the
  checks, and return a report. Runnable as a module for long local runs and the R=1/W=1 demo.
- **`test_consistency.py`** — the pytest entry point, marked `integration`, ~20 s by default
  (`CHECKER_DURATION` overrides). Asserts a clean report under N=3/R=2/W=2.

Values written are unique per (worker, sequence) so a read can be traced back to the exact put
that produced it. Operation invoke/completion times use `time.perf_counter()`, not
`time.monotonic()`: on Windows monotonic has ~15 ms resolution, which would tie a read's invoke
to a concurrent write's completion and fabricate a happens-before edge, producing spurious
read-your-writes violations.

### Two things the orchestration has to get right

Both are consequences of DistroCache having **no background anti-entropy** — convergence is
restored only by read repair, which runs on the client read path:

1. **A healed node must be reachable by the coordinator before convergence is judged.** A node
   the nemesis relaunched can be *serving* (its own process is up) yet unreachable over the
   coordinator's cached gRPC channel, which is still reconnecting. Read repair skips a replica
   whose read vote errors, so such a node would never converge. So after the run, with clients
   stopped, the runner does a quiet **clean restart of every node** — `cluster.restart` waits for
   the coordinator to reach the node again, confirming every channel. Data persists on disk, and
   the restarts run one at a time so a quorum always remains.
2. **Convergence is eventual, so it is driven, not sampled.** The runner reads every key over
   several passes until the replicas agree or a generous bound is hit, rather than reading once
   and hoping. A system that genuinely cannot converge still terminates and is reported as a
   divergence.

## The five checks

1. **No acked write lost.** Each key's final highest-versioned replica entry has a version `>=`
   the highest acked write for that key — unless an acked delete at `>=` that version explains a
   final tombstone.
2. **No fabricated read.** Every value a read returns was the value of some put *attempt* for that
   key (acked or not: a put that returned 503 may still have written to a replica).
3. **Read-your-writes.** A read **hit** must return a version `>=` the highest acked write/delete
   that *completed before the read was invoked*; returning older means a committed event was
   missed. A read **miss** is flagged only when a committed value precedes it and *no delete was
   ever attempted* for that key before the read finished — because then nothing could have
   produced the tombstone a 404 implies. This asymmetry keeps the check sound under concurrency (a
   404 carries no version, so a concurrent delete that acked just after the read began must not be
   treated as a violation).
4. **No resurrected delete.** No read returns a value with a version `<=` an acked delete that
   completed before the read started.
5. **Replicas converge.** After the final read pass, every replica holding a key agrees on its
   version and tombstone flag.

The checks are config-agnostic, so *where* we assert decides the meaning:

- **R + W > N** (R=2/W=2/N=3): quorum overlap holds, so every check must pass — the integration
  test asserts a clean report.
- **R + W <= N** (R=1/W=1/N=3): the guarantees do not hold, so the checker is *expected* to report
  violations (stale reads, resurrected deletes, occasionally a lost acked write). This is the proof
  that the checker can fail — a checker that cannot fail proves nothing. Recorded in
  `docs/benchmarks/checker-runs.md`.

## Proving the checker can fail

Two independent proofs:

- **Unit** (`tests/test_checker.py`): each check is run against a hand-built passing history and a
  hand-built violating history, so its ability to detect each specific violation is tested
  directly and **deterministically**, in the default (non-integration) test job.
- **End-to-end** (`runner.main` with `--read-quorum 1 --write-quorum 1`): the same checker against
  a real cluster under the nemesis reports real violations. This is probabilistic — DistroCache's
  background replication is fast, so a stale read only appears when a read happens to hit a
  not-yet-replicated replica — so about four runs in five report violations; the recorded result
  and repeated runs make the demonstration dependable. (This is why the *deterministic* unit
  proof exists alongside it.)

## Consequences

- The checker finds interleavings no hand-written scenario would, and its R=1/W=1 run is a
  standing demonstration that the R+W>N guarantee is real and necessary.
- It is timing-sensitive by nature. The duration is configurable; CI runs a short pass, and longer
  local runs (3 × 2 min) are recorded in `docs/benchmarks/`.
- It does not test linearizability (DistroCache does not claim it) — only the quorum and
  last-writer-wins safety properties above.
