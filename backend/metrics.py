from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

REQUEST_COUNT = Counter(
    "distrocache_requests_total",
    "Total number of requests",
    ["operation"],
)
HIT_COUNT = Counter(
    "distrocache_hits_total",
    "Total number of cache hits",
    ["operation"],
)
LATENCY = Histogram(
    "distrocache_request_latency_seconds",
    "Request latency in seconds",
    ["operation"],
)
REQUEST_FAILURES = Counter(
    "distrocache_request_failures_total",
    "Requests the coordinator could not complete. Misses are not counted.",
    ["operation", "reason"],
)
NODE_STATUS = Gauge(
    "distrocache_node_up",
    "Node health status",
    ["node"],
)
ACTIVE_NODES = Gauge(
    "distrocache_active_nodes",
    "Active node count",
)
NODE_REGISTRATIONS = Counter(
    "distrocache_node_registrations_total",
    "Node registrations",
)
NODE_DEREGISTRATIONS = Counter(
    "distrocache_node_deregistrations_total",
    "Node deregistrations",
)
HEARTBEAT_FAILURES = Counter(
    "distrocache_heartbeat_failures_total",
    "Heartbeat failures",
)
REBALANCE_DURATION = Histogram(
    "distrocache_rebalance_duration_seconds",
    "Rebalance duration in seconds",
)
REBALANCE_EVENTS = Counter(
    "distrocache_rebalance_events_total",
    "Rebalance events",
)
REPLICA_RESTORES = Counter(
    "distrocache_replica_restores_total",
    "Replica restoration events",
)
