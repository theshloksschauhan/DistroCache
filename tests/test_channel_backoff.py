import asyncio

import grpc
import pytest

from backend.app import Coordinator, Settings


def test_coordinator_channels_use_bounded_reconnect_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_channel(target: str, options: object = None) -> object:
        captured["options"] = options
        raise RuntimeError("do not connect")

    monkeypatch.setattr(grpc.aio, "insecure_channel", fake_channel)
    coordinator = Coordinator(Settings(node_addresses=[]))

    with pytest.raises(RuntimeError, match="do not connect"):
        asyncio.run(coordinator.clients.get_stub("127.0.0.1:9"))

    options = dict(captured["options"])  # type: ignore[arg-type]
    assert options["grpc.min_reconnect_backoff_ms"] == 100
    assert options["grpc.initial_reconnect_backoff_ms"] == 200
    assert options["grpc.max_reconnect_backoff_ms"] == 2000
