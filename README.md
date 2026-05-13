# DistroCache

Distributed in-memory key-value store with consistent hashing, replication, and a real-time monitoring dashboard.

## Architecture
- gRPC cache nodes handle reads/writes.
- FastAPI backend routes requests using consistent hashing and tracks cluster health.
- React dashboard shows live node status via WebSockets.
- Prometheus scrapes backend metrics.
- Nodes persist data to SQLite and rebalance replicas on membership changes.

## Quick start (Docker)
```bash
docker compose up --build
```

Services:
- API: http://localhost:8000
- Dashboard: http://localhost:3000
- Prometheus: http://localhost:9090

## Local dev
1) Generate gRPC code
```bash
python scripts/gen_proto.py
```

2) Start nodes (repeat per node)
```bash
set NODE_ID=node1
set GRPC_PORT=50051
python -m node.app
```

3) Start backend
```bash
set NODE_ADDRESSES=node1:50051,node2:50051,node3:50051
set REPLICATION_FACTOR=3
uvicorn backend.app:app --reload
```

4) Start frontend
```bash
cd frontend
npm install
npm run dev
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
- `GET /metrics`
- `WS /ws/cluster`

## Tests
```bash
pytest
```

## Vercel deployment
- Frontend: deploy the `frontend` directory as a Vite app.
- API: deploy using the serverless function in `api/index.py` with `vercel.json`.

Suggested flow:
1) Create a Vercel project for the API (root directory = repo root) and deploy.
2) Create a Vercel project for the frontend (root directory = `frontend`) and deploy.
3) Set `VITE_API_URL` in the frontend project to the deployed API URL.

Recommended API env vars:
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
- `MAX_ITEMS` to cap SQLite entries (0 disables LRU eviction).

Notes:
- The API uses gRPC to talk to cache nodes, so you still need the gRPC nodes running elsewhere (e.g., Docker or a VM).
- Vercel serverless does not support WebSockets. The dashboard automatically falls back to polling `/health`.
# DistroCache
