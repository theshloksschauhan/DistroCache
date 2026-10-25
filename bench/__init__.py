"""Benchmark harness for DistroCache.

An async load generator (`loadgen`) drives a running cluster and measures throughput and latency
percentiles; `run_bench` starts 3-node clusters at each (R, W) setting, sweeps value size,
concurrency and a healthy-vs-degraded condition, and writes raw JSON plus matplotlib charts to
`docs/benchmarks/`. These are developer tools, run by hand — not part of CI.
"""
