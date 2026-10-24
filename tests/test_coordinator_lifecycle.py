import time

from fastapi.testclient import TestClient

from backend.app import Coordinator, Settings, create_app


def test_lifespan_cancels_background_tasks() -> None:
    app = create_app(Settings(node_addresses=[], health_interval=0.05, heartbeat_interval=0.05))
    coordinator: Coordinator = app.state.coordinator

    with TestClient(app) as client:
        assert client.get("/membership/nodes").status_code == 200
        tasks = list(coordinator._tasks)
        assert len(tasks) == 2
        assert not any(task.done() for task in tasks), "a background loop exited early"

    assert all(task.cancelled() for task in tasks), "a background loop outlived the app"


def test_background_loops_survive_one_failed_iteration() -> None:
    app = create_app(Settings(node_addresses=[], health_interval=0.05, heartbeat_interval=0.05))
    coordinator: Coordinator = app.state.coordinator
    health_calls = {"n": 0}
    prune_calls = {"n": 0}
    live_addresses = coordinator.registry.live_addresses
    prune_expired = coordinator.registry.prune_expired

    async def flaky_live() -> list[str]:
        health_calls["n"] += 1
        if health_calls["n"] == 1:
            raise RuntimeError("health blew up")
        return await live_addresses()

    async def flaky_prune() -> list[object]:
        prune_calls["n"] += 1
        if prune_calls["n"] == 1:
            raise RuntimeError("prune blew up")
        return await prune_expired()

    coordinator.registry.live_addresses = flaky_live  # type: ignore[method-assign]
    coordinator.registry.prune_expired = flaky_prune  # type: ignore[method-assign]

    with TestClient(app):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if health_calls["n"] >= 2 and prune_calls["n"] >= 2:
                break
            time.sleep(0.05)
        assert health_calls["n"] >= 2, "health loop died on the first error"
        assert prune_calls["n"] >= 2, "prune loop died on the first error"
        assert not any(task.done() for task in coordinator._tasks)


def test_each_app_gets_its_own_coordinator() -> None:
    first = create_app(Settings(node_addresses=["127.0.0.1:1"]))
    second = create_app(Settings(node_addresses=[]))
    assert first.state.coordinator is not second.state.coordinator
