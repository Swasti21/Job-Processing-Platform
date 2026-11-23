"""No-Kafka mode (BUS=local): worker tasks run inside the API process and read from an asyncio.Queue.

Same state machine, same database, same retries. Useful when you want to develop the API or UI
without a broker, and as a fallback if Kafka gives you trouble on demo day.
"""
import asyncio
import logging

from . import config
from .bus import LocalPublisher
from .worker import handle_message

log = logging.getLogger("local")


async def _consumer(publisher: LocalPublisher, name: str) -> None:
    while True:
        topic, _key, payload = await publisher.queue.get()
        try:
            if topic != config.TOPIC_DLQ:
                # Retry messages wait for their back-off without blocking the other consumers.
                await handle_message(payload, publisher, name)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("local worker error")
        finally:
            publisher.queue.task_done()


def start_local_workers(publisher: LocalPublisher, count: int | None = None) -> list[asyncio.Task]:
    count = count or config.WORKER_CONCURRENCY
    return [asyncio.create_task(_consumer(publisher, f"local-{i}")) for i in range(count)]


async def drain(publisher: LocalPublisher, worker_id: str = "test-worker") -> int:
    """Process everything currently queued, including retries it creates. Returns messages handled."""
    handled = 0
    while not publisher.queue.empty():
        topic, _key, payload = publisher.queue.get_nowait()
        if topic == config.TOPIC_DLQ:
            continue
        payload = {k: v for k, v in payload.items() if k != "not_before"}  # skip back-off in tests
        await handle_message(payload, publisher, worker_id)
        handled += 1
    return handled
