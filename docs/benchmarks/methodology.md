# Benchmark methodology

How the numbers in `benchmarks.json` and the charts here were produced, and how to read them.

## What is measured

An async load generator (`bench/loadgen.py`) opens `concurrency` coroutines against the
coordinator, each looping a 50/50 GET/POST mix over a 50-key working set (pre-populated). Every
request is timed with `time.perf_counter()`. A **warmup window is excluded** from the reported
statistics (channel setup, SQLite page-cache warmup). Reported per scenario:

- **ops/sec** — successful operations per measured second (a GET counts as success on 200 or 404;
  a POST on 200; a 503 below-quorum refusal or a transport error counts as an error, not an op).
- **p50 / p95 / p99 latency (ms)** — nearest-rank percentiles over successful-request latencies.
- **errors** — requests that were not successful (almost all are 503s under degraded quorum).

## The matrix

`bench/run_bench.py` sweeps, with **N = 3** throughout:

- quorum `(R, W)` ∈ {(1,1), (2,2), (3,3)}
- value size ∈ {100 B, 1 KB}
- concurrency ∈ {8, 32}
- condition ∈ {healthy, one node killed}

A fresh 3-node localhost cluster is started per `(R, W)`; for the degraded condition one node is
killed and the run waits until the coordinator marks it down. Clusters run in a temp dir, so no
`.db` files or logs touch the repo.

Reproduce:

```bash
python -m bench.run_bench --duration 8 --warmup 3
```

## Machine

Captured in `benchmarks.json`:

- OS: Windows 11 (10.0.26200)
- CPU: Intel Core (Family 6 Model 140 — 11th-gen "Tiger Lake"), 8 logical cores
- RAM: 15.7 GB
- Python: 3.12.4

All processes (coordinator, three nodes, load generator) share this one machine over loopback, so
these are **relative** numbers for comparing quorum settings and failure modes on one box — not
absolute capacity figures for a distributed deployment.

## Headline numbers

See `throughput_by_quorum.png`, `latency_by_quorum.png`, and
`throughput_healthy_vs_degraded.png`. Representative figures from the recorded run
(`benchmarks.json`, 8 s measured / 3 s warmup per scenario):

| Setting | Throughput (healthy, 1 KB, c=8) | p99 latency (healthy, 1 KB, c=32) | One node killed (1 KB, c=32) |
|---------|--------------------------------:|----------------------------------:|------------------------------|
| R=1/W=1 | ~265 ops/s | ~646 ms | ~346 ops/s (serves) |
| R=2/W=2 | ~315 ops/s | ~874 ms | ~254 ops/s (serves) |
| R=3/W=3 | ~287 ops/s | ~805 ms | **0 ops/s (all 503 — no quorum)** |

Peak throughput overall was ~390 ops/s (R=1/W=1, 100 B, concurrency 8). Throughput is in the low
hundreds of ops/sec across the board — the coordinator, not the storage layer, is the ceiling (see
below). Notably, with one node killed R=1/W=1 and R=2/W=2 often serve *faster* than healthy,
because the coordinator fans out to and waits on fewer replicas — a direct illustration of the
latency cost of the extra durability a full-cluster write buys.

## Honest reading of the results — where the time goes

- **The coordinator is the bottleneck, by design.** It is a single Python `asyncio` process that
  fans every request out to the replicas, awaits the quorum, and (for reads) schedules repair. It
  does not scale across cores. At concurrency 32 the tail latency climbs sharply (p99 approaching
  ~1 s) because requests queue at this one event loop — throughput is flat or *lower* at 32 than
  at 8, the classic sign of a saturated single server past its useful concurrency.
- **SQLite per node adds real write cost.** Each replica commits to its own SQLite file in WAL
  mode (`synchronous=NORMAL`). Writes are durable to the OS and cost more than the reads.
- **Higher quorums cost latency, not safety for free.** Moving from R=1/W=1 to R=2/W=2 to
  R=3/W=3 raises latency (the coordinator waits for more replicas) while throughput changes
  little — the price of read-your-writes (`R + W > N`) and of waiting for the slowest replica in
  the quorum.
- **Degraded quorum is an availability cliff, not a slope.** With one node killed, R=1/W=1 and
  R=2/W=2 keep serving (a quorum of the surviving two still forms), but **R=3/W=3 cannot form a
  quorum at all — every request 503s** (0 ops/sec, all errors). That is the explicit cost of
  `W = N`: maximum durability, zero fault tolerance. It is the clearest single data point for why
  the default is `R = W = 2`.
- **Localhost flatters latency and punishes nothing for network.** On real networks the quorum
  fan-out and the wait-for-slowest effect would be larger; these numbers understate them.
