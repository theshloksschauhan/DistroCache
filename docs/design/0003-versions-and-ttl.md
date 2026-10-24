# 0003: Coordinator-assigned versions (HLC) and TTL

Status: accepted (Phase 1, Stage 3)

## Context

Entry versions were per-node counters: `_put_sync` read the node's own stored version and
stored `version + 1`. Two nodes could reach the same count for different writes, and a count
said nothing about order across nodes. The audit's reproduction
(`test_versions::test_rebalance_does_not_replace_newer_value_with_older`) shows the damage: a
key is written three times (every replica reaches count 3), one replica is killed, the key is
deleted and rewritten (the live replicas start a fresh row at count 1), and when the killed
replica returns its rebalance pushes its count-3 copy of the *old* value, which wins over the
count-1 *new* value. An older value overwrites a newer one.

Stage 2 also left two write hazards: a timed-out write is retried, and the node applied the
retry as a second write (a new, higher count), so a retry was not idempotent. And TTL was
computed on each node independently (`now + TTL_SECONDS` at the moment each replica stored the
key), so replicas of one key could expire at slightly different times.

## Decision

### A hybrid logical clock in the coordinator

`distrocache_common/hlc.py` holds an `HLC` value `(physical_ms, logical, coordinator_id)` and a
`HybridClock`. `HLC` is a frozen `order=True` dataclass, so it compares as that tuple:
wall-clock milliseconds first, a logical counter next, and the coordinator id last as a
stable tie-break. The id makes the order total even when several coordinators mint versions
in the same millisecond (none do yet; the field is here so the rule does not change when they
do).

`HybridClock.tick()` returns a strictly increasing `HLC`:

- if the wall clock has advanced past the last stamp, take the new wall time and reset the
  logical counter to 0;
- otherwise (same millisecond, or the clock went backwards) keep the last wall time and
  increment the logical counter.

Either branch yields a value greater than the previous one, so the clock never goes backwards
even if the OS clock does. `tick()` performs no `await`, so the single-threaded event loop runs
it to completion without interleaving; no lock is needed.

The coordinator assigns a version **once per HTTP request** and reuses it for every replica and
every retry. The request object is built once per replica with that version, and `_rpc` resends
the *same* request on a retry, so a retried write carries the identical version.

### The version and expiry travel in the proto

`Version { int64 physical_ms; int64 logical; string coordinator_id; }` is a new proto message.
`PutRequest`, `DeleteRequest`, `PutReplicaRequest`, and `Entry` carry a `Version`, and
`PutRequest`/`DeleteRequest` also carry an absolute `expires_at`. Nodes store exactly the
version they are handed and never invent one.

`Put`, `Delete`, and `PutReplica` now return a `WriteStatus`:

- `APPLIED` — the incoming version was newer than what was stored (or nothing was stored), so
  the node wrote it.
- `ALREADY_CURRENT` — the node already held this version or a newer one, so it did nothing.
- `ERROR` — the store raised; `error` carries the text.

The coordinator counts `APPLIED` and `ALREADY_CURRENT` as acks and `ERROR` (or an RPC
exception) as a failed vote. Because a retried write carries the same version, the second
attempt finds the row already current and returns `ALREADY_CURRENT`, which still acks: the
retry is now idempotent.

### Last-writer-wins everywhere

`Storage.upsert(key, value, expires_at, version)` is the single write path for both `Put` and
`PutReplica`. It reads the stored version, and writes only if the incoming version is strictly
greater; it returns whether it wrote. Node-to-node rebalance uses the same rule, so a stale
copy pushed during a rebalance can no longer overwrite a newer one.

`Delete` still removes the row in this stage (no tombstone yet), so a replica that missed a
delete can still resurrect the value — that bug keeps its `xfail` until Stage 5. The delete
already carries a version so Stage 5 only has to change behaviour, not the proto.

### TTL is computed once, by the coordinator

The coordinator computes an absolute `expires_at = now + ttl` and sends it to every replica, so
all replicas of a key expire together. `ttl` is the request body's optional `ttl_seconds` when
present, otherwise the coordinator's `TTL_SECONDS` (default 0 = no expiry). A node stores the
`expires_at` it is given and treats a key as absent once `now >= expires_at`; it no longer
computes expiry itself, and the node `TTL_SECONDS` variable is gone.

### The SQLite schema is versioned

`PRAGMA user_version` records the schema version (now 1). On open, a database whose
`user_version` does not match is dropped and recreated, with a logged warning — it is a cache,
so throwing it away is correct and cheaper than migrating. A fresh file (version 0) is just
created, without the warning. The new schema stores the version as three columns
(`version_ms`, `version_logical`, `version_coordinator`) compared as a tuple in Python.

## Consequences

- An older value can no longer overwrite a newer one through rebalance; the Stage 3 `xfail` is
  removed and the test passes.
- A retried write is idempotent: the duplicate apply documented in 0002 is gone.
- `Put`/`Delete`/`PutReplica` responses changed from `ok`/`applied` booleans to a `WriteStatus`.
  `GetEntry`/`ListEntries` `Entry.version` changed from an `int64` count to a `Version` message.
  The integration harness's `node_entry` now returns the version tuple, and the version-shape
  assertions in `test_harness` check the value and that replicas agree, not a literal count.
- `POST /kv/{key}` accepts an optional `ttl_seconds`. The 200/404/503 bodies are unchanged in
  this stage (Stage 4 adds `acks` and the chosen version).
- Changing `proto/cache.proto` means regenerating the stubs with `scripts/gen_proto.py`; the
  regenerated files are committed.
- The node `TTL_SECONDS` variable is removed; `TTL_SECONDS` is now a coordinator variable and
  moves to the backend service in `docker-compose.yml`.

## Alternatives considered

- **Keep per-node counters but gossip a max.** Still not an order across nodes, and it needs a
  background protocol. An HLC is a single comparable value per write.
- **A pure logical (Lamport) clock.** Correct for order but drifts away from wall-clock time, so
  it could not also drive TTL. The physical component keeps versions close to real time.
- **Pack the HLC into one 64-bit integer** (`physical_ms << 16 | logical`). One column and a
  trivial SQL comparison, but it caps the logical counter at 16 bits and drops the coordinator
  id, which the multi-coordinator tie-break needs. Three columns compared in Python are
  explicit and have no cap.
- **Migrate the SQLite schema in place.** Pointless for a cache: dropping and refilling from
  replicas is simpler and the data is not authoritative.
- **Let each node keep computing TTL.** Replicas of one key would expire at different instants,
  so a read quorum could see a key alive on one replica and expired on another.

## Carried forward

- **Stage 4:** `acks` and the chosen version in the 200 response body; a directly observable
  miss metric; parallel fan-out with R/W quorums.
- **Stage 5:** `Delete` writes a tombstone with its version instead of removing the row; the
  delete-resurrection `xfail` is removed then.
- **Stage 7:** a `node_id` label on the rebalance metrics, replacing the `ring_version`
  inference in the version tests.
