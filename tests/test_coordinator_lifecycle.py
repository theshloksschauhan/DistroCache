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


def test_each_app_gets_its_own_coordinator() -> None:
    first = create_app(Settings(node_addresses=["127.0.0.1:1"]))
    second = create_app(Settings(node_addresses=[]))
    assert first.state.coordinator is not second.state.coordinator
