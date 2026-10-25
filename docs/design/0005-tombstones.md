# 0005: Tombstones

Status: accepted (Phase 1, Stage 5)

## Context

Through Stage 4 a delete removed the row. A replica that missed the delete (it was down, or the
delete did not reach it) still held the value, and nothing recorded that the key had been
deleted. The reproduction `test_tombstones::test_delete_is_not_undone_by_a_replica_that_missed_it`
shows it: write a key, kill one replica, delete the key on the survivors, restart the replica —
and a read sees the value again. The removal carried no version, so it could not win over the
older value the returning replica still held; a rebalance or read could resurrect it.

Last-writer-wins (0003) needs a delete to be a *version*, not an absence. A higher version that
says "deleted" must beat a lower version that says "here is the value".

## Decision

### A delete is a versioned tombstone

`Delete` no longer removes the row; it writes a **tombstone**: the row is kept with the
coordinator's fresh HLC version, an empty value, a `tombstone` flag, and a `deleted_at` timestamp.
It goes through the same quorum path as a put (W acks required), so a delete is as durable as a
write. Last-writer-wins then applies uniformly: a tombstone with a higher version beats a live
value with a lower one, and a later write with a still-higher version beats the tombstone.

The write path is one storage method, `upsert(key, value, expires_at, version, tombstone)`, shared
by `Put` (tombstone false), `Delete` (tombstone true, empty value), and `PutReplica` (tombstone
from the request). `PutReplicaRequest` and `Entry` gain a `tombstone` flag so rebalance and read
repair can transfer a tombstone, not just a value.

### Reads see tombstones but return 404

A read must *see* a tombstone to let it win over an older value, so `GetResponse` gains a
`tombstone` flag (and already carries the version). The read quorum collects found values **and**
tombstones as responses, picks the highest version among them, and if the winner is a tombstone
the read is a 404 (and counts as a miss). A key with no row anywhere is still a 404. This is why
the stale-replica's old value can no longer resurrect a delete: the tombstone's higher version
wins the read.

### Tombstones are garbage-collected, not kept forever

A tombstone is kept for `TOMBSTONE_GC_SECONDS` (default 3600) and then purged on the next prune
(on read and on write). Until then it occupies a row. LRU eviction (`MAX_ITEMS`) only evicts live
rows; a tombstone is reclaimed solely by GC, so a recent delete can never be evicted out from
under a replica that still needs it to suppress an older value.

### The resurrection trade-off

A tombstone that has been GC'd is gone. If a replica was down for longer than
`TOMBSTONE_GC_SECONDS`, missed both the delete and its tombstone's whole lifetime, and then comes
back with the old value, nothing is left to out-version it, so the value can resurrect. The window
is a tunable: longer is safer and costs disk; `TOMBSTONE_GC_SECONDS` must exceed the longest
outage a deployment wants to tolerate. This is the standard tombstone trade-off (Cassandra's
`gc_grace_seconds`).

### Schema

`user_version` goes to 2; the cache table gains `tombstone INTEGER NOT NULL DEFAULT 0` and
`deleted_at REAL`. A mismatch drops and recreates as before.

## Consequences

- A missed delete no longer resurrects within the GC window; the Stage 5 `xfail` is removed.
- `Delete` is now a quorum write (W acks), not a best-effort removal; its 200/503 behaviour
  matches `POST`.
- gRPC: `GetResponse`, `PutReplicaRequest`, and `Entry` gained a `tombstone` flag.
- New config `TOMBSTONE_GC_SECONDS` (node); a tombstone costs a row until it is GC'd.
- A delete older than the GC window can resurrect if a replica was offline for that entire window
  (documented trade-off).

## Alternatives considered

- **Keep removing the row and track deletes elsewhere** (a separate deleted-keys set). That is a
  second store with its own consistency problem; a tombstone reuses the versioned row and the LWW
  rule already in place.
- **Never GC tombstones.** No resurrection ever, but deleted keys accumulate forever. A tunable
  window bounds the disk cost and matches what Dynamo-style stores do.
- **GC by absolute `gc_at` stamped by the coordinator.** Slightly simpler prune, but a tombstone
  transferred late (rebalance, read repair) would already be near expiry on arrival. Stamping
  `deleted_at` when each node first stores the tombstone gives every replica the full window.

## Carried forward

- **Stage 6 (same PR):** read repair propagates the winning version — value *or* tombstone — to
  replicas that answered with an older version or a miss.
- **Stage 7:** rebalance transfers tombstones (it already pushes every listed row); the
  compare-and-delete keeps a tombstone that is younger than the GC window.
