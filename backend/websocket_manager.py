from __future__ import annotations

import json
from typing import Any, List

from fastapi import WebSocket


class WebSocketManager:
    def __init__(self) -> None:
        self._connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._connections.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self._connections:
            self._connections.remove(websocket)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        if not self._connections:
            return
        message = json.dumps(payload)
        living = []
        for socket in self._connections:
            try:
                await socket.send_text(message)
                living.append(socket)
            except Exception:
                continue
        self._connections = living
