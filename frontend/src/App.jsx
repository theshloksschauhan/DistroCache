import React, { useEffect, useMemo, useState } from "react";

const DEFAULT_API = import.meta.env.VITE_API_URL || "http://localhost:8000";

const statusLabel = (ok) => (ok ? "Healthy" : "Unreachable");

export default function App() {
  const [nodes, setNodes] = useState({});
  const [lastUpdate, setLastUpdate] = useState(null);
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
        setLastUpdate(new Date());
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
    pollTimer = window.setInterval(fetchHealth, 3000);

    return () => {
      isMounted = false;
      if (pollTimer) window.clearInterval(pollTimer);
      if (socket) socket.close();
    };
  }, [apiUrl]);

  const nodeEntries = Object.entries(nodes);

  return (
    <div className="app">
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
    </div>
  );
}
