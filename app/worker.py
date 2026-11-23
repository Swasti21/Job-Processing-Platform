"""Kafka consumer worker.   Run:  python -m app.worker

Scale horizontally by starting more copies (same consumer group); Kafka spreads the
topic's partitions across them:  docker compose up --scale worker=3
"""
import asyncio
import json
import logging
import os
import signal
import socket
import time

from . import config
from .processing import process_message

log = logging.getLogger("worker")
WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"


async def handle_message(payload: dict, publisher, worker_id: str = WORKER_ID) -> str:
    """Shared by the Kafka worker and the local runner."""
    wait = payload.get("not_before", 0) - time.time()
    if wait > 0:  # retry back-off: hold the message until it is due
        await asyncio.sleep(min(wait, 60))
    return await process_message(payload, publisher, worker_id)


async def run_worker() -> None:
    from aiokafka import AIOKafkaConsumer
    from .bus import KafkaPublisher, ensure_topics
    from .db import init_db

    await init_db()
    await ensure_topics()
    publisher = KafkaPublisher()
    await publisher.start()
    consumer = AIOKafkaConsumer(
        config.TOPIC_SUBMITTED, config.TOPIC_RETRY,
        bootstrap_servers=config.KAFKA_BOOTSTRAP, group_id=config.CONSUMER_GROUP,
        enable_auto_commit=False,          # commit only after the DB work is done (at-least-once)
        auto_offset_reset="earliest",
        max_poll_interval_ms=600_000,
    )
    await consumer.start()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    sem = asyncio.Semaphore(config.WORKER_CONCURRENCY)

    async def handle(raw) -> None:
        async with sem:
            try:
                payload = json.loads(raw.value)
            except ValueError:
                log.error("dropping malformed message at %s/%s", raw.topic, raw.offset)
                return
            outcome = await handle_message(payload, publisher)
            log.debug("job %s -> %s", payload.get("job_id"), outcome)

    log.info("worker %s started (concurrency=%s)", WORKER_ID, config.WORKER_CONCURRENCY)
    try:
        while not stop.is_set():
            batches = await consumer.getmany(timeout_ms=1000, max_records=config.WORKER_CONCURRENCY * 4)
            messages = [m for msgs in batches.values() for m in msgs]
            if not messages:
                continue
            # Any exception here (DB down, Kafka publish failed) crashes the worker on purpose:
            # offsets stay uncommitted, the container restarts, and the messages are redelivered.
            await asyncio.gather(*(handle(m) for m in messages))
            await consumer.commit()
    finally:
        await consumer.stop()
        await publisher.stop()


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(name)s %(message)s")
    asyncio.run(run_worker())
