# DistroCache

[![CI](https://github.com/theshloksschauhan/DistroCache/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/theshloksschauhan/DistroCache/actions/workflows/ci.yml)

Distributed key-value cache with consistent hashing, replication, and a real-time monitoring dashboard.

## Architecture
- gRPC cache nodes handle reads/writes. Each node stores entries in its own SQLite file (WAL mode).
- FastAPI backend routes requests using consistent hashing and tracks cluster health.
- Nodes register and heartbeat with the backend, and rebalance replicas when membership changes.
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
- `GET /kv/{key}`
- `POST /kv/{key}` with JSON `{ "value": "..." }`
- `DELETE /kv/{key}`
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
pytest --cov
ruff check
ruff format --check
mypy
```

Install the git hooks with `pre-commit install`.

## Configuration

API env vars:
- `NODE_ADDRESSES` to a comma-separated list of reachable gRPC nodes.
- `REPLICATION_FACTOR` to the desired replication count.
- `HEALTH_INTERVAL` for cluster polling interval.
- `HEARTBEAT_INTERVAL` for membership pruning checks.
- `HEARTBEAT_TIMEOUT` for marking nodes down.
- `REQUEST_TIMEOUT` gRPC request timeout in seconds.
- `RETRY_ATTEMPTS` retry count for gRPC calls.
- `RETRY_BACKOFF` base retry backoff in seconds.
- `CORS_ORIGINS` to the frontend URL (or `*` for development).

Node env vars:
- `BACKEND_URL` to the API base URL for membership heartbeats.
- `NODE_ADDRESS` to the gRPC address other nodes should use (defaults to `NODE_ID:GRPC_PORT`).
- `HEARTBEAT_INTERVAL` for node heartbeat frequency in seconds.
- `REPLICATION_FACTOR` to align with the API replication factor.
- `GRPC_TIMEOUT` gRPC timeout for replica transfers.
- `STORAGE_PATH` to the SQLite file for persistence.
- `TTL_SECONDS` to expire keys (0 disables TTL).
- `MAX_ITEMS` to cap SQLite entries, evicting the least recently accessed (0 disables eviction).

Frontend:
- `VITE_API_URL` to the API base URL (build-time).

## Known limitations (being addressed)
- Reads return the first replica that has the key; there is no read quorum.
- Writes succeed if any one replica acknowledges them.
- Deletes leave no tombstones, so a replica that missed a delete can still serve the deleted value.
- Entry versions are per-node counters, not cluster-wide.
- A dead replica can make `GET` return HTTP 500 instead of trying the remaining replicas.
- A single coordinator holds membership and routes every request.
- No authentication or TLS on the HTTP API or between nodes.
- No benchmarks yet.

## License
[MIT](LICENSE)
