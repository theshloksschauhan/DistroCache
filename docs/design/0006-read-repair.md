# 0006: Read repair

Status: accepted (Phase 1, Stage 6)

## Context

A read quorum (0004) returns the freshest of R responses, but it leaves the stale replicas stale.
A replica that was down for a write, or lost one, keeps serving an old version (or a miss) until a
rebalance happens to touch the key. With `R + W > N` the client still gets the latest value, but
the cluster does not converge on its own between membership changes. Anti-entropy on the read path
is the cheap, standard fix: every read already fetched each replica's version, so it already knows
which replicas are behind.

## Decision

### Repair after the response, never before it

After a read, the coordinator asynchronously writes the winning version — a value **or** a
tombstone — to every replica that answered with an older version or a miss. It is a detached
background task, so it never delays the response the client already received.

The read fans out to all N replicas (0004). The response is built from the first R responses as
before. The repair task then awaits **all** the replica replies, including ones that arrive after
the R-th, so a slow replica is still repaired. It computes the highest version across every
responder and, for each responder whose version is lower or who missed, sends a `PutReplica` with
the winner's value, version, expiry, and tombstone flag. Replicas that failed the read are not
touched; a later read or a rebalance handles them. Last-writer-wins makes the push idempotent and
safe even if several reads repair at once.

### On/off and a metric

`READ_REPAIR_ENABLED` (default true) turns it off without a redeploy of logic. Each repair push
increments `distrocache_read_repairs_total`, so the dashboard and tests can see repair happening.

## Consequences

- After one read of a key, every responding replica converges to the newest version without
  waiting for a rebalance. The Stage 6 integration test stops a replica, writes, restarts it, reads
  once, and sees that replica hold the newest version.
- A read now issues up to N−1 extra background writes when replicas disagree. They are bounded by
  the read's fan-out, run after the response, and are skipped entirely when all replicas already
  agree (the common case).
- New metric `distrocache_read_repairs_total`; new config `READ_REPAIR_ENABLED`.

## Alternatives considered

- **Repair from only the first R responses.** Misses a stale replica that happened to be the
  (R+1)-th to answer. Awaiting all replies in the background costs nothing on the response path and
  converges every responder.
- **Repair synchronously before responding.** Simpler to reason about but adds the slowest
  replica's write latency to every read that found a disagreement — exactly what the quorum read
  avoided.
- **Rely only on rebalance/anti-entropy.** Keys that never move between membership changes would
  stay divergent indefinitely; read repair converges hot keys continuously.
- **Full Merkle-tree anti-entropy.** The thorough background repair, but a large build-out; it is
  explicitly future work. Read repair covers the keys that are actually read.

## Carried forward

- **Stage 7:** rebalance (membership change) remains the mechanism that converges keys that are
  never read; read repair and rebalance are complementary.
- **Future:** Merkle anti-entropy for keys that are neither read nor moved.
