"""Async load generator: hammer a running coordinator and measure throughput and latency.

Spawns `concurrency` coroutines that each loop issuing a mix of GET/POST against a small working
set, timing every request. A warmup window is excluded from the reported statistics so JIT-like
warmup effects (channel setup, SQLite page cache) do not skew the numbers.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

import httpx


@dataclass
class LatencyStats:
    count: int
    errors: int
    ops_per_sec: float
    p50_ms: float
    p95_ms: float
    p99_ms: float

    def as_dict(self) -> dict[str, float | int]:
        return {
            "count": self.count,
            "errors": self.errors,
            "ops_per_sec": round(self.ops_per_sec, 1),
            "p50_ms": round(self.p50_ms, 2),
            "p95_ms": round(self.p95_ms, 2),
            "p99_ms": round(self.p99_ms, 2),
        }


@dataclass
class _Samples:
    latencies_ms: list[float] = field(default_factory=list)
    errors: int = 0


def _percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    # Nearest-rank percentile.
    rank = max(0, min(len(sorted_values) - 1, int(round(q * (len(sorted_values) - 1)))))
    return sorted_values[rank]


async def _worker(
    worker_id: int,
    client: httpx.AsyncClient,
    url: str,
    keys: list[str],
    value: str,
    read_fraction: float,
    deadline: float,
    warmup_until: float,
    samples: _Samples,
) -> None:
    rng = _Lcg(seed=worker_id * 2654435761 + 1)
    i = 0
    while time.perf_counter() < deadline:
        key = keys[rng.next() % len(keys)]
        is_read = (rng.next() % 1000) / 1000.0 < read_fraction
        start = time.perf_counter()
        try:
            if is_read:
                resp = await client.get(f"{url}/kv/{key}")
                ok = resp.status_code in (200, 404)
            else:
                resp = await client.post(f"{url}/kv/{key}", json={"value": value})
                ok = resp.status_code == 200
        except httpx.HTTPError:
            ok = False
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        if time.perf_counter() >= warmup_until:
            if ok:
                samples.latencies_ms.append(elapsed_ms)
            else:
                samples.errors += 1
        i += 1


class _Lcg:
    """A tiny deterministic PRNG so a worker's op stream is reproducible without locking."""

    def __init__(self, seed: int) -> None:
        self._state = seed & 0xFFFFFFFF

    def next(self) -> int:
        self._state = (1103515245 * self._state + 12345) & 0x7FFFFFFF
        return self._state


async def generate_load(
    url: str,
    *,
    concurrency: int,
    value_size: int,
    duration_s: float,
    warmup_s: float,
    read_fraction: float = 0.5,
    key_count: int = 50,
) -> LatencyStats:
    """Run the load for `duration_s` (including `warmup_s`) and return excluded-warmup stats."""
    keys = [f"bench-{i}" for i in range(key_count)]
    value = "x" * value_size
    samples = _Samples()
    # No keep-alive cap issue: a dedicated async client with a generous connection pool.
    limits = httpx.Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(timeout=10.0, limits=limits) as client:
        # Pre-populate the working set so reads hit existing keys.
        await asyncio.gather(*(client.post(f"{url}/kv/{k}", json={"value": value}) for k in keys))
        start = time.perf_counter()
        deadline = start + warmup_s + duration_s
        warmup_until = start + warmup_s
        await asyncio.gather(
            *(
                _worker(
                    wid, client, url, keys, value, read_fraction, deadline, warmup_until, samples
                )
                for wid in range(concurrency)
            )
        )
    measured_s = duration_s
    latencies = sorted(samples.latencies_ms)
    return LatencyStats(
        count=len(latencies),
        errors=samples.errors,
        ops_per_sec=len(latencies) / measured_s if measured_s > 0 else 0.0,
        p50_ms=_percentile(latencies, 0.50),
        p95_ms=_percentile(latencies, 0.95),
        p99_ms=_percentile(latencies, 0.99),
    )
