# DistroCache

[![CI](https://github.com/theshloksschauhan/DistroCache/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/theshloksschauhan/DistroCache/actions/workflows/ci.yml)

Distributed key-value cache with consistent hashing, replication, and a real-time monitoring dashboard.

## Architecture
- gRPC cache nodes handle reads/writes. Each node stores entries in its own SQLite file (WAL mode).
- FastAPI backend routes requests using consistent hashing and tracks cluster health.
- Nodes register and heartbeat with the backend, and rebalance replicas when membership changes.
- The coordinator stamps every write with a hybrid logical clock version and an absolute TTL, and
  replicas apply writes last-writer-wins, so a stale copy never overwrites a newer one.
- Reads and writes use tunable quorums (N, R, W) with parallel fan-out: a write returns once W
  replicas ack, a read once R respond and returns the highest version seen.
- A delete writes a versioned tombstone (kept for `TOMBSTONE_GC_SECONDS`), so a replica that
  missed it cannot resurrect the value. After a read, read repair pushes the winning version
  (value or tombstone) to any replica that answered stale or missing.
- React dashboard receives cluster updates over a WebSocket and also polls `/health` (every 3 s) and `/membership/nodes` (every 5 s).
- Prometheus scrapes backend metrics.
- `distrocache_common/` holds the hash ring, gRPC client pool, and generated protobuf stubs shared by the backend and nodes.

## Quick start (Docker)
```bash
docker compose up --build
```

Services:
- API: http://localhost:8000
- Dashboard: http://localhost:3000
- Prometheus: http://localhost:9090

## Local dev
Run every command from the repo root.

1) Install dependencies
```bash
pip install -r requirements-dev.txt
```

2) Start the backend

PowerShell:
```powershell
$env:NODE_ADDRESSES="127.0.0.1:50051,127.0.0.1:50052"
$env:REPLICATION_FACTOR="2"
python -m uvicorn backend.app:app --reload
```

bash:
```bash
export NODE_ADDRESSES=127.0.0.1:50051,127.0.0.1:50052
export REPLICATION_FACTOR=2
python -m uvicorn backend.app:app --reload
```

3) Start nodes (one terminal per node; change the ID, port, address, and storage path for each)

PowerShell:
```powershell
$env:NODE_ID="node1"
$env:GRPC_PORT="50051"
$env:NODE_ADDRESS="127.0.0.1:50051"
$env:BACKEND_URL="http://127.0.0.1:8000"
$env:REPLICATION_FACTOR="2"
$env:STORAGE_PATH="node1.db"
python -m node.app
```

bash:
```bash
export NODE_ID=node1
export GRPC_PORT=50051
export NODE_ADDRESS=127.0.0.1:50051
export BACKEND_URL=http://127.0.0.1:8000
export REPLICATION_FACTOR=2
export STORAGE_PATH=node1.db
python -m node.app
```

4) Start the frontend
```bash
cd frontend
npm ci
npm run dev
```

The generated gRPC code in `distrocache_common/generated/` is committed. After editing `proto/cache.proto`, regenerate it with:
```bash
python scripts/gen_proto.py
```

## API
- `GET /kv/{key}` — 200 `{key, value, node, acks, version}`, 404 if no replica holds it, 503 if
  fewer than R replicas respond.
- `POST /kv/{key}` with JSON `{ "value": "...", "ttl_seconds": 60 }` (`ttl_seconds` optional;
  falls back to the coordinator's `TTL_SECONDS`) — 200 `{key, replicas, acks, version}` once W
  replicas ack, 503 otherwise.
- `DELETE /kv/{key}` — writes a tombstone under the same quorum and body shape as `POST`.
- `GET /health`
- `GET /membership/nodes`
- `POST /membership/register`
- `POST /membership/heartbeat`
- `POST /membership/deregister`
- `POST /membership/rebalance`
- `GET /metrics`
- `WS /ws/cluster`

## Tests and checks
```bash
pytest --cov          # unit tests
pytest -m integration # starts a real coordinator and nodes on localhost ports
ruff check
ruff format --check
mypy
```

Integration tests run each cluster in a temporary directory, so no `.db` files are written
into the repo. See [docs/design/0001-integration-harness.md](docs/design/0001-integration-harness.md).

Install the git hooks with `pre-commit install`.

## Configuration

API env vars:
- `NODE_ADDRESSES` to a comma-separated list of reachable gRPC nodes.
- `REPLICATION_FACTOR` (N) to the desired replication count.
- `READ_QUORUM` (R) replicas that must respond to a read (default 2; `1 <= R <= N`).
- `WRITE_QUORUM` (W) replicas that must ack a write (default 2; `1 <= W <= N`). `R + W > N`
  gives read-your-writes.
- `READ_REPAIR_ENABLED` whether a read repairs stale replicas in the background (default `true`).
- `HEALTH_INTERVAL` for cluster polling interval.
- `HEARTBEAT_INTERVAL` for membership pruning checks.
- `HEARTBEAT_TIMEOUT` for marking nodes down.
- `REQUEST_TIMEOUT` gRPC request timeout in seconds.
- `RETRY_ATTEMPTS` retry count for gRPC calls.
- `RETRY_BACKOFF` base retry backoff in seconds.
- `TTL_SECONDS` default key lifetime in seconds; the coordinator sets an absolute expiry sent to
  every replica (0 disables TTL). A request body may override it per write with `ttl_seconds`.
- `COORDINATOR_ID` identifier mixed into every write version (default `coordinator`); keep it
  unique per coordinator once there is more than one.
- `CORS_ORIGINS` to the frontend URL (or `*` for development).

Node env vars:
- `BACKEND_URL` to the API base URL for membership heartbeats.
- `NODE_ADDRESS` to the gRPC address other nodes should use (defaults to `NODE_ID:GRPC_PORT`).
- `GRPC_HOST` interface the gRPC server binds to (default `0.0.0.0`; use `127.0.0.1` for local-only).
- `HEARTBEAT_INTERVAL` for node heartbeat frequency in seconds.
- `REPLICATION_FACTOR` to align with the API replication factor.
- `GRPC_TIMEOUT` gRPC timeout for replica transfers.
- `STORAGE_PATH` to the SQLite file for persistence.
- `MAX_ITEMS` to cap SQLite entries, evicting the least recently accessed (0 disables eviction).
- `TOMBSTONE_GC_SECONDS` how long a delete's tombstone is kept before it is purged (default 3600).

Frontend:
- `VITE_API_URL` to the API base URL (build-time).

## Known limitations (being addressed)
Open correctness limitations. The ones with a reproducing test are marked `xfail` in `tests/`.

- A delete can still resurrect if a replica is offline for longer than `TOMBSTONE_GC_SECONDS`
  (the tombstone is GC'd before that replica comes back). Tunable trade-off, documented in
  design record 0005.
- Nodes listed in `NODE_ADDRESSES` get a second `seed-N` membership record that is later marked down.
- A rebalance can delete a write that reaches a node while that node is handing the key off.
- A single coordinator holds membership and routes every request.
- No authentication or TLS on the HTTP API or between nodes.
- No benchmarks yet.

## License
[MIT](LICENSE)
