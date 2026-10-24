# 0002: Error handling

Status: accepted (Phase 1, Stage 2)

## Context

A gRPC error from one replica escaped `GET /kv/{key}` as HTTP 500, and that path never
incremented `distrocache_request_failures_total`. `PUT` and `DELETE` caught per-replica
errors and then discarded them, including `ok=false`. Every exception was retried. One
exception in the coordinator's health loop, or in the membership prune loop, ended that
loop for the life of the process. gRPC's default max reconnect backoff is 120s, so a
channel that watched a node die could ignore the restart for two minutes.

Quorum sizes are unchanged: a write still succeeds when any one replica acks, and a read
still returns the first replica that has the key. Stage 4 changes both.

## Decision

### Votes

Each replica gives one vote.

- **Ack.** The RPC returns. For `PUT` and `DELETE`, `ok` is true. For `GET`, `found` true
  or false both count: the replica answered.
- **Failure.** The RPC raises after its retries, or `PUT`/`DELETE` returns `ok=false`.

`required` is 1 for GET, PUT, and DELETE. The 503 body carries the number so Stage 4 can
change it to R and W without changing the shape. Fan-out stays sequential, in preference-list
order, and still contacts every replica so a write is attempted on the whole preference list.

- **GET** returns 200 on the first `found=true`, with the same body as before. If at least
  one replica answered and none had the key, it returns 404 `{"detail": "Key not found"}`.
  If every replica failed, it returns 503.
- **PUT and DELETE** return 200 when `acks >= 1`, with the same body as before
  (`failures` is the number of failed votes). They return 503 when `acks == 0`.
- **No live nodes** is the same 503, with an empty `errors` list.

The 503 body is these three fields:

```json
{"acks": 0, "required": 1, "errors": [{"node": "127.0.0.1:50051", "error": "UNAVAILABLE: connection refused"}]}
```

`errors` has one entry per failed vote. A gRPC error is the status name plus `details()`.
`ok=false` uses the response's `error` string.

A failed vote is logged at WARNING with the operation, the replica address, and that same
text. The node logs a failed rebalance `PutReplica` the same way.

### Retries

Each attempt's deadline is `REQUEST_TIMEOUT`. A retry happens only when the status is
`UNAVAILABLE` or `DEADLINE_EXCEEDED`, up to `RETRY_ATTEMPTS` (default 2) with the existing
backoff. Any other exception fails the vote immediately. `ok=false` is a vote result, so
it is not retried.

Writes are retried too. A `DEADLINE_EXCEEDED` means the replica may have applied the write
before the coordinator stopped waiting, and the retry sends that write again. Today the
node treats the retry as a new write and bumps its local version counter.

### Metrics

`distrocache_request_failures_total` has a `reason` label and counts only real failures,
once per failed HTTP request:

- `reason="unavailable"` for every 503 above
- `reason="internal"` when a stored value is not valid UTF-8 (HTTP 500)

A GET miss does not increment it. A 200 that survived a dead replica does not either; that
replica is in the log and, on a 503, in `errors`. Misses are derivable as get requests
minus hits minus `unavailable` failures minus `internal` failures.

### Reconnect backoff

The coordinator's channel pool sets `grpc.min_reconnect_backoff_ms=100`,
`grpc.initial_reconnect_backoff_ms=200`, and `grpc.max_reconnect_backoff_ms=2000`. The
default max is 120s. The 100ms floor keeps a dead node from being probed more than 10 times
a second, the 200ms first wait fits inside one `REQUEST_TIMEOUT` (1s), and the 2s cap means
a restarted node is dialable again within a few seconds even after repeated failures. A 5s
cap would also be a few seconds, and it would sit on top of the sequential fan-out, which
already spends a full deadline on each attempt. Node-to-node channels keep the gRPC defaults.

### Loops

Each iteration of the health loop and the membership prune loop is wrapped so an exception
is logged and the loop sleeps, then continues. `CancelledError` is a `BaseException`, so
shutdown still cancels the tasks.

### Worst-case latency, and a false 404

Both are fixed by Stage 4.

With sequential fan-out the coordinator waits for every replica, and every attempt can use
the whole deadline. Between attempts it sleeps `RETRY_BACKOFF * 2^i`.

```
per_replica = RETRY_ATTEMPTS * REQUEST_TIMEOUT
              + RETRY_BACKOFF * (2^(RETRY_ATTEMPTS - 1) - 1)
worst       = N * per_replica
```

Defaults are N=3, `RETRY_ATTEMPTS=2`, `REQUEST_TIMEOUT=1`, `RETRY_BACKOFF=0.1`, so
per replica is 2.1s and a request is 6.3s. A GET that finds the key on the first try
returns after one RPC. Stage 4 sends the N calls in parallel and returns when R or W have
acked, so the response waits about one replica's worst case and the rest finish in the
background.

Under `required=1`, a GET returns 404 when any replica answers with a miss and no contacted
replica returned the key. Replicas that failed are skipped. If the replicas holding the key
are the ones that failed, and a replica without it answers, the client gets a false 404.
Stage 4's read quorum waits for R answers and picks the highest version, so one miss cannot
decide the read.

## Consequences

- `GET` no longer returns 500 because one replica is down.
- The 503 body for `/kv/{key}` is `{acks, required, errors}`. It was a `detail` string.
- 200 bodies are unchanged. 404 is unchanged.
- `distrocache_request_failures_total` gained a `reason` label and stopped counting GET
  misses. Scrapes that assumed the old series, or treated the counter as "gets that
  returned no value", need the formula above.
- A retried write can be applied twice until Stage 3.

## Alternatives considered

- **Wrap the 503 fields in FastAPI's `detail`.** One exception type would do it. The body
  the callers need is `acks`, `required`, and `errors`.
- **Keep counting GET 404 as a failure, with `reason="not_found"`.** A miss is a successful
  answer. The requests and hits counters, minus the real failures, already give the miss count.
- **Stop retrying writes.** That avoids the double-apply. A brief `UNAVAILABLE` would then
  fail a write that a second try would have landed. Stage 3 makes the retry safe; until then
  the retry stays, and the double-apply is documented here.
- **A 5s reconnect cap.** Still a few seconds, and it adds to the per-attempt deadline the
  sequential fan-out already pays. 2s is the cap.

## Carried forward

- **Stage 3:** the coordinator assigns the version once per HTTP request and reuses it for
  every replica and every retry. A replica that already holds that exact version returns
  `ALREADY_CURRENT`, and the coordinator counts that as an ack. That is what makes a
  retried write safe.
- **Stage 4:** make misses directly observable, with a `distrocache_misses_total` counter
  or an `outcome` label on `distrocache_requests_total`, instead of deriving them from
  requests, hits, and failures.
- **Stage 7:** add a `node_id` label to the rebalance metrics, and switch the version tests
  from the `ring_version` inference to that label.
- **Later:** on Windows, put harness child processes in a Job Object with kill-on-close, so
  a `pytest-timeout` exit cannot orphan them.
