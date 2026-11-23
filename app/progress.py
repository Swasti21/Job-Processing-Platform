"""Real-time progress: workers NOTIFY on Postgres, the API LISTENs and fans out to WebSocket clients."""
import asyncio
import json
import logging

import asyncpg
from fastapi import WebSocket

from .db import asyncpg_dsn

log = logging.getLogger("progress")
CHANNEL = "batch_progress"


class ProgressHub:
    def __init__(self):
        self._clients: dict[str, set[WebSocket]] = {}

    async def connect(self, batch_id: str, ws: WebSocket) -> None:
        await ws.accept()
        self._clients.setdefault(batch_id, set()).add(ws)

    def disconnect(self, batch_id: str, ws: WebSocket) -> None:
        self._clients.get(batch_id, set()).discard(ws)

    async def broadcast(self, batch_id: str, payload: dict) -> None:
        for ws in list(self._clients.get(batch_id, ())):
            try:
                await ws.send_json(payload)
            except Exception:  # noqa: BLE001  (client went away)
                self.disconnect(batch_id, ws)

    async def on_notify(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except ValueError:
            return
        await self.broadcast(data.get("batch_id", ""), data)


async def listen_forever(hub: ProgressHub) -> None:
    """Keep one dedicated asyncpg connection LISTENing; reconnect on failure."""
    while True:
        conn = None
        try:
            conn = await asyncpg.connect(asyncpg_dsn())
            await conn.add_listener(
                CHANNEL, lambda _c, _pid, _ch, payload: asyncio.create_task(hub.on_notify(payload)))
            log.info("listening on postgres channel %s", CHANNEL)
            while not conn.is_closed():
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            if conn and not conn.is_closed():
                await conn.close()
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("listener error, retrying: %s", exc)
            await asyncio.sleep(2)
