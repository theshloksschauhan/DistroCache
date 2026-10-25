# 0007: Membership and safe rebalance

Status: accepted (Phase 1, Stage 7)

## Context

Two bugs remained, and the membership model behind them was fragile.

- **Duplicate seed records** (`test_membership::test_seeded_nodes_have_one_live_record_each`).
  Seeds from `NODE_ADDRESSES` were stored under synthetic ids `seed-1..N`. A real node registered
  under its own id, so one address ended up with two records — the seed (which later went down,
  having never heartbeat) beside the live node.
- **A rebalance lost a concurrent write** (`test_rebalance::test_rebalance_keeps_a_write_that_lands_during_the_push`).
  A node handing a key off read a snapshot, pushed the row, then deleted it — unconditionally. A
  client write that landed after the snapshot but before the delete was removed.
- **Churn on short blips.** `ring_version` bumped whenever a node went down (prune) or came back
  (heartbeat down→up). Every bump made every node rebalance, so a two-second network hiccup
  triggered a full cluster rebalance, and the ring (the preference list) shrank and grew with
  liveness.

## Decision

### One record per address; liveness is separate from ring membership

`MembershipRegistry` now keys records by **address**, the stable identity in the hash ring. A
seed is just a record whose id is `seed` until a real node registers at that address and replaces
it in place — so an address always has exactly one record.

Two notions are kept apart:

- **Ring membership** — the set of addresses in the hash ring. It changes only when an address
  **registers** for the first time, **deregisters** (graceful leave), or is removed after a long
  `DEAD_TIMEOUT` with no heartbeat. Only these bump `ring_version`, and only a `ring_version`
  change makes nodes rebalance. A node that merely went quiet stays in the ring, so a blip no
  longer rebalances the cluster, and the preference list is stable through it.
- **Liveness** — a per-record status for routing and display: `down` when the coordinator's gRPC
  health probe is currently failing, else `suspect` when no heartbeat has arrived within
  `HEARTBEAT_TIMEOUT`, else `up`. A heartbeat marks the record up again optimistically; the health
  loop confirms it. Liveness never changes `ring_version`.

The coordinator routes against the full preference list (every ring member) and **skips members
whose status is `down`**, counting each as a failed vote immediately instead of waiting out a
gRPC timeout. Quorum then decides as usual: with one of three replicas down, a write still needs
W=2 acks from the two live ones. The down member keeps its place in the ring and catches up by
read repair or rebalance when it returns.

### Safe rebalance

`_rebalance_once` still pushes each local row to the key's new owners, but a hand-off deletes the
local row only when **both** hold:

1. the node is no longer an owner of the key, **and** at least `WRITE_QUORUM` new owners returned
   `APPLIED` or `ALREADY_CURRENT` (the key is safely replicated before the copy is dropped); and
2. **compare-and-delete**: the stored version is still the one that was read into the batch. A
   client write that changed the row since the snapshot bumped its version, so the delete is
   skipped and the newer write survives.

Rows are processed in batches (`REBALANCE_BATCH_SIZE`); a `PutReplicaBatch` RPC was considered but
not added — per-row pushes plus compare-and-delete already give correctness, and a cache's working
set is small. Over-replication (keeping a copy when fewer than W owners confirmed) is safe; losing
a write is not, so the rule errs toward keeping the copy.

### Metrics carry a node id; the version tests use it

The rebalance metrics gain a `node_id` label, so a test can wait for *one* node's rebalance
instead of inferring "only X rebalanced" from an unchanged `ring_version`. The version, stale-read,
and read-repair tests now wait on `distrocache_rebalance_events_total{node_id=...}` and no longer
assert on `ring_version`; the settle heuristic is kept only for the unavoidable startup wait.

### Bounded state

`recent_rebalances` is capped at the last 50 entries, and the health loop prunes `cluster_state`
entries for addresses that have left the ring, so neither grows without bound.

## Consequences

- Seeds and real nodes share one record per address; the Stage 7 `xfail` is removed.
- A rebalance can no longer delete a row that changed under it; the rebalance-race `xfail` is
  removed.
- A short outage no longer rebalances the cluster; the ring is stable through it and the
  coordinator skips the down member as an immediate failed vote.
- Membership API: the `status` field is now `up`/`suspect`/`down`; `deregister` is keyed by
  address (the node sends its address). `NODE_ADDRESSES` members still appear immediately as ring
  members. `HEARTBEAT_TIMEOUT` now means "suspect after", and a new `DEAD_TIMEOUT` means "removed
  from the ring after".
- Rebalance metrics gained a `node_id` label. Node config gained `WRITE_QUORUM` (for the hand-off
  rule) and the coordinator gained `DEAD_TIMEOUT`.
- The integration harness's preference list is computed from all ring members (not just `up`
  ones), matching the coordinator, and tests that kill a node assert it is still a ring member
  rather than still `up`.

## Alternatives considered

- **Key records by node id (as before) and dedupe seeds by scanning addresses.** Address is the
  real identity in the ring; keying by it removes the duplicate class of bug entirely instead of
  patching it.
- **Shrink the ring the moment a node goes down.** Simple, but it reshuffles ownership on every
  blip and causes exactly the churn this stage removes. A down node keeps its ring slot.
- **Roll the hand-off into a transaction / two-phase delete.** Heavier machinery; compare-and-delete
  plus the W-owner check gives the needed safety with one extra version check at delete time.
- **Add `PutReplicaBatch`.** A latency win for huge transfers, but it complicates the proto and the
  node for little gain on a cache-sized dataset; per-row push is kept.

## Carry forward / future work

- Hinted handoff and Merkle anti-entropy for replicas that are down past the tombstone/again
  window (out of scope for Phase 1).
- Multiple coordinators (the HLC already carries a coordinator id for this).
