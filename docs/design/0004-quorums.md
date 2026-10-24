# 0004: Tunable quorums (N, R, W) with parallel fan-out

Status: accepted (Phase 1, Stage 4)

## Context

Stages 2 and 3 kept the Stage-0 quorum: a write succeeded as soon as one replica acked, and a
read returned the first replica that held the key. Design record 0002 spelled out the two
consequences and promised Stage 4 would fix them:

- **A false 404.** A read contacted replicas in order and returned the first answer. If the
  replicas holding the key were down and a replica without it answered, the client got a 404
  even though the key existed (`test_quorum::test_stale_replica_does_not_hide_newer_value`
  returned a stale value for the same reason — the first answer won regardless of version).
- **A write with no durability floor.** One ack was enough, so a write could "succeed" on a
  single replica and be lost when that replica died
  (`test_quorum::test_write_without_quorum_is_rejected`).
- **A 6.3 s worst case.** Fan-out was sequential and waited for every replica, each with its own
  retry budget.

## Decision

### Configurable N, R, W

`REPLICATION_FACTOR` (N, default 3), `READ_QUORUM` (R, default 2), and `WRITE_QUORUM` (W, default
2) are coordinator settings. The coordinator validates `1 <= R <= N` and `1 <= W <= N` when it is
built and raises otherwise. It logs whether `R + W > N` (reads and writes overlap, so a read sees
the latest acked write) or not (a read may miss it). The defaults give `R + W = 4 > 3`.

### Writes: parallel fan-out, return at W

A write is sent to all N replicas at once (`asyncio.create_task` per replica). The coordinator
returns 200 as soon as W replicas ack, and the still-pending writes are **detached** to finish in
the background — they are not cancelled, so a replica that was briefly slow still receives the
write. If every replica finishes and fewer than W acked, it returns 503. A failed quorum write is
**not rolled back**: the replicas that did ack keep the value. This is the standard Dynamo
trade-off — rolling back would need a second round that can itself fail, and read repair plus
last-writer-wins converge the cluster anyway.

`APPLIED` and `ALREADY_CURRENT` both ack (from 0003, this is what makes a retry idempotent); an
`ERROR` status or an RPC exception is a failed vote.

### Reads: parallel fan-out, return at R, highest version wins

A read is sent to all N replicas at once. Each reply is a *response*: a value (`found`) or a miss
(`found=false`, which includes an expired entry the node has dropped). A failed RPC is not a
response. The coordinator waits until R replicas have responded, then picks the **highest
version** among the values it has; a read with no value (all misses) is a 404. If fewer than R
replicas respond because the rest failed, it returns 503. With `R + W > N`, any R responses
overlap the last W-acked write, so the highest version among them is the latest write.

`GetResponse` now carries the entry's `version` and `expires_at`, so the coordinator can compare
versions without a second RPC. `Get` still bumps `last_access`, so reads keep feeding LRU
eviction. The read picks the freshest of the first R responses; a newer value that arrives after
the R-th response does not change the answer (Stage 6 read-repair will push the winner to the
stale replicas).

### Response bodies and the miss metric

200 bodies gain `acks` (replicas that answered/acked) and `version` (the chosen version, as
`{physical_ms, logical, coordinator_id}`). The 503 body keeps `{acks, required, errors}`, where
`required` is the configured quorum. A GET miss increments a new `distrocache_misses_total`
counter, so misses are directly observable instead of being derived from requests minus hits
minus failures (closing the 0002 carry-forward).

## Consequences

- The false 404 and the stale-read are gone; both Stage 4 `xfail`s are removed.
- A write needs W acks, so a single surviving replica no longer "succeeds" a write.
- Latency is about one replica's worst case instead of the sum over N, because fan-out is
  parallel and the response does not wait for the slow or dead replicas past the quorum.
- `GET`/`POST` 200 bodies gained `acks` and `version`. The gRPC `GetResponse` gained `version`
  and `expires_at`.
- A new metric `distrocache_misses_total{operation}` is exported.
- Design record 0002's "worst-case latency and a false 404" section is now resolved; see the note
  added there.

## Alternatives considered

- **Cancel the extra writes once W ack.** Saves a little work but loses writes to replicas that
  were only briefly slow, and makes the cluster less consistent until the next repair. Detaching
  them is cheap and strictly better for durability.
- **Roll back a failed quorum write.** Needs a second round that can also fail partway, and it
  fights last-writer-wins. Dynamo-style stores leave the partial write and let repair converge.
- **Read via `GetEntry` instead of extending `GetResponse`.** `GetEntry` does not bump
  `last_access`, so reads would stop feeding LRU eviction. Adding the version to `GetResponse`
  keeps one read path that both returns the version and records the access.
- **Clamp R/W down to the number of live replicas at request time.** That would silently weaken
  the guarantee during a partial outage. R and W are validated against N once; at request time
  the quorum is only capped at the preference-list length (which is N, or fewer only in a cluster
  smaller than N).

## Carried forward

- **Stage 5:** `Delete` writes a tombstone so a missed delete cannot resurrect; reads treat a
  tombstone as a 404 (the read path already picks the highest version, so a tombstone with the
  highest version will win and map to 404).
- **Stage 6:** read repair pushes the winning version to replicas that answered with an older
  version or a miss, including replicas that answer after the R-th response.
- **Stage 7:** a `node_id` label on the rebalance metrics.
