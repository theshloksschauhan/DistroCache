import React, { useEffect, useMemo, useState } from "react";

const DEFAULT_API = import.meta.env.VITE_API_URL || "http://localhost:8000";
const DEMO_URL = "https://github.com/theshloksschauhan/DistroCache#demo";

const statusLabel = (ok) => (ok ? "Healthy" : "Unreachable");

export default function App() {
  const [nodes, setNodes] = useState({});
  const [lastUpdate, setLastUpdate] = useState(null);
  const [membership, setMembership] = useState([]);
  const [ringVersion, setRingVersion] = useState(0);
  const [rebalances, setRebalances] = useState([]);
  const [noCluster, setNoCluster] = useState(false);
  const apiUrl = useMemo(() => DEFAULT_API, []);

  useEffect(() => {
    let socket;
    let isMounted = true;
    let pollTimer;

    const fetchHealth = async () => {
      try {
        const response = await fetch(`${apiUrl}/health`);
        if (!response.ok) return;
        const data = await response.json();
        if (!isMounted) return;
        setNodes(data.nodes || {});
        setNoCluster(Object.keys(data.nodes || {}).length === 0);
        setLastUpdate(new Date());
      } catch (error) {
        return;
      }
    };

    const fetchMembership = async () => {
      try {
        const response = await fetch(`${apiUrl}/membership/nodes`);
        if (!response.ok) return;
        const data = await response.json();
        if (!isMounted) return;
        setMembership(data.nodes || []);
        setRingVersion(data.ring_version || 0);
      } catch (error) {
        return;
      }
    };

    const connect = () => {
      socket = new WebSocket(apiUrl.replace("http", "ws") + "/ws/cluster");
      socket.onmessage = (event) => {
        if (!isMounted) return;
        const data = JSON.parse(event.data);
        if (data.type === "cluster") {
          setNodes(data.nodes || {});
          setRebalances(data.rebalances || []);
          setLastUpdate(new Date());
        }
      };
      socket.onerror = () => {
        if (socket) socket.close();
      };
      socket.onclose = () => {
        if (isMounted) {
          setTimeout(connect, 1500);
        }
      };
    };

    connect();
    fetchHealth();
    fetchMembership();
    pollTimer = window.setInterval(fetchHealth, 3000);
    const membershipTimer = window.setInterval(fetchMembership, 5000);

    return () => {
      isMounted = false;
      if (pollTimer) window.clearInterval(pollTimer);
      if (membershipTimer) window.clearInterval(membershipTimer);
      if (socket) socket.close();
    };
  }, [apiUrl]);

  const nodeEntries = Object.entries(nodes);

  return (
    <div className="app">
      {noCluster && (
        <div className="notice" role="status">
          This hosted dashboard has no cluster attached. The cache nodes are long-running gRPC
          servers that Vercel cannot host. Watch the{" "}
          <a href={DEMO_URL} target="_blank" rel="noreferrer">
            recorded demo
          </a>{" "}
          or run <code>docker compose up</code> locally.
        </div>
      )}
      <header className="hero">
        <div>
          <p className="eyebrow">DistroCache</p>
          <h1>Distributed Cache Control Room</h1>
          <p className="subtitle">
            Live health, latency, and replication awareness for the cluster.
          </p>
        </div>
        <div className="summary">
          <div>
            <span>Nodes</span>
            <strong>{nodeEntries.length}</strong>
          </div>
          <div>
            <span>Last update</span>
            <strong>{lastUpdate ? lastUpdate.toLocaleTimeString() : "--"}</strong>
          </div>
          <div>
            <span>Ring version</span>
            <strong>{ringVersion}</strong>
          </div>
        </div>
      </header>

      <section className="grid">
        {nodeEntries.length === 0 && (
          <div className="card empty">Waiting for cluster data...</div>
        )}
        {nodeEntries.map(([node, info]) => (
          <div className={`card ${info.ok ? "ok" : "down"}`} key={node}>
            <div className="card-header">
              <h2>{node}</h2>
              <span className="badge">{statusLabel(info.ok)}</span>
            </div>
            <div className="card-body">
              <p>
                <span>Latency</span>
                <strong>
                  {info.latency_ms ? `${info.latency_ms.toFixed(1)} ms` : "--"}
                </strong>
              </p>
              <p>
                <span>Last seen</span>
                <strong>
                  {info.last_seen ? new Date(info.last_seen * 1000).toLocaleTimeString() : "--"}
                </strong>
              </p>
            </div>
          </div>
        ))}
      </section>

      <section className="grid">
        <div className="card">
          <div className="card-header">
            <h2>Membership</h2>
            <span className="badge">{membership.length} nodes</span>
          </div>
          <div className="card-body">
            {membership.length === 0 && <p>Waiting for membership data...</p>}
            {membership.map((node) => (
              <p key={node.node_id}>
                <span>{node.address}</span>
                <strong>{node.status}</strong>
                <span>keys: {node.key_count}</span>
              </p>
            ))}
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <h2>Recent rebalances</h2>
            <span className="badge">{rebalances.length}</span>
          </div>
          <div className="card-body">
            {rebalances.length === 0 && <p>No rebalance activity yet.</p>}
            {rebalances.map((rebalance) => (
              <p key={`${rebalance.node_id}-${rebalance.timestamp}`}>
                <span>{rebalance.node_id}</span>
                <strong>{rebalance.keys_moved} moved</strong>
                <span>{Number(rebalance.duration_s || 0).toFixed(2)}s</span>
              </p>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}
