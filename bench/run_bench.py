"""Run the benchmark matrix against real clusters and write JSON + charts to docs/benchmarks/.

Matrix (N=3): (R,W) in {(1,1),(2,2),(3,3)} x value {100 B, 1 KB} x concurrency {8, 32} x
condition {healthy, one node killed}. Run by hand; not part of CI.

    python -m bench.run_bench --duration 5 --warmup 2
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bench.loadgen import generate_load  # noqa: E402
from tests.integration.harness import Cluster  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "docs" / "benchmarks"

RW_SETTINGS = [(1, 1), (2, 2), (3, 3)]
VALUE_SIZES = [100, 1024]
CONCURRENCIES = [8, 32]


def machine_specs() -> dict[str, str]:
    return {
        "os": platform.platform(),
        "cpu": platform.processor() or platform.machine(),
        "cpu_count": str(__import__("os").cpu_count()),
        "ram_gb": _total_ram_gb(),
        "python": platform.python_version(),
    }


def _total_ram_gb() -> str:
    try:
        if sys.platform == "win32":
            import ctypes

            class _MemStatus(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            stat = _MemStatus()
            stat.dwLength = ctypes.sizeof(_MemStatus)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))  # type: ignore[attr-defined]
            return f"{stat.ullTotalPhys / 1024**3:.1f}"
        import os

        return f"{os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 1024**3:.1f}"
    except Exception:
        return "unknown"


async def _run_matrix(duration: float, warmup: float, workroot: Path) -> dict:
    results: list[dict] = []
    for r, w in RW_SETTINGS:
        cluster = Cluster(
            workroot / f"rw{r}{w}",
            replication_factor=3,
            dead_timeout=600,
            coordinator_env={"READ_QUORUM": str(r), "WRITE_QUORUM": str(w)},
        )
        try:
            cluster.start(3)
            victim = next(iter(cluster.nodes.values()))
            for condition in ("healthy", "one_killed"):
                for value_size in VALUE_SIZES:
                    for concurrency in CONCURRENCIES:
                        if condition == "one_killed":
                            # Ensure the victim is down before *every* degraded scenario, not just
                            # once: a single kill can be briefly undone by a transient, which would
                            # silently turn a degraded data point into a healthy one.
                            _ensure_down(cluster, victim)
                        stats = await generate_load(
                            cluster.url,
                            concurrency=concurrency,
                            value_size=value_size,
                            duration_s=duration,
                            warmup_s=warmup,
                        )
                        row = {
                            "n": 3,
                            "r": r,
                            "w": w,
                            "condition": condition,
                            "value_size": value_size,
                            "concurrency": concurrency,
                            **stats.as_dict(),
                        }
                        results.append(row)
                        print(
                            f"R={r} W={w} {condition:10} v={value_size:>4}B c={concurrency:>2} "
                            f"-> {row['ops_per_sec']:>8.1f} ops/s  p50={row['p50_ms']:.1f} "
                            f"p99={row['p99_ms']:.1f}ms  err={row['errors']}",
                            flush=True,
                        )
        finally:
            cluster.close(print_logs=False)
    return {
        "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "machine": machine_specs(),
        "params": {"duration_s": duration, "warmup_s": warmup, "read_fraction": 0.5},
        "results": results,
    }


def _ensure_down(cluster: Cluster, victim, timeout: float = 15.0) -> None:
    """Kill the victim if it is somehow running, then wait until the coordinator marks it down.

    Called before every degraded scenario so the 'one node killed' condition is always genuine.
    """
    if victim.process.running:
        cluster.kill(victim)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            health = cluster.http.get(f"{cluster.url}/health").json()["nodes"]
            state = health.get(victim.address)
            if state is None or not state["ok"]:
                return
        except Exception:
            pass
        time.sleep(0.3)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="DistroCache benchmark matrix")
    parser.add_argument("--duration", type=float, default=5.0, help="measured seconds per scenario")
    parser.add_argument("--warmup", type=float, default=2.0, help="warmup seconds (excluded)")
    parser.add_argument("--no-charts", action="store_true", help="write JSON only")
    args = parser.parse_args(argv)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    # Clusters run in a temp dir so no .db files or logs ever land in the repo.
    workroot = Path(tempfile.mkdtemp(prefix="distrocache-bench-"))
    data = asyncio.run(_run_matrix(args.duration, args.warmup, workroot))
    json_path = OUT_DIR / "benchmarks.json"
    json_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"\nwrote {json_path}")
    if not args.no_charts:
        from bench.charts import write_charts

        paths = write_charts(data, OUT_DIR)
        for p in paths:
            print(f"wrote {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
