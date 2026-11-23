"""Message bus abstraction: real Kafka for docker compose, an in-process queue for tests / no-Kafka dev."""
import asyncio
import json
import logging

from . import config

log = logging.getLogger("bus")


class KafkaPublisher:
    def __init__(self):
        self._producer = None

    async def start(self):
        from aiokafka import AIOKafkaProducer
        self._producer = AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP, acks="all", enable_idempotence=True, linger_ms=20)
        await self._producer.start()

    async def stop(self):
        if self._producer:
            await self._producer.stop()

    async def send(self, topic: str, key: str, payload: dict) -> None:
        await self._producer.send_and_wait(topic, key=key.encode(), value=json.dumps(payload).encode())

    async def send_many(self, topic: str, items: list[tuple[str, dict]]) -> None:
        # Enqueue everything first, then wait for acks: much faster than send_and_wait in a loop.
        futures = [await self._producer.send(topic, key=k.encode(), value=json.dumps(p).encode())
                   for k, p in items]
        await asyncio.gather(*futures)


class LocalPublisher:
    """Same interface, backed by an asyncio.Queue. Also records everything sent (handy in tests)."""

    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.sent: list[tuple[str, str, dict]] = []

    async def start(self):
        pass

    async def stop(self):
        pass

    async def send(self, topic: str, key: str, payload: dict) -> None:
        self.sent.append((topic, key, payload))
        await self.queue.put((topic, key, payload))

    async def send_many(self, topic: str, items: list[tuple[str, dict]]) -> None:
        for key, payload in items:
            await self.send(topic, key, payload)


async def ensure_topics(retries: int = 30) -> None:
    """Create topics if missing (idempotent). Retries while the broker is starting."""
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    topics = [NewTopic(t, num_partitions=config.TOPIC_PARTITIONS, replication_factor=1)
              for t in (config.TOPIC_SUBMITTED, config.TOPIC_RETRY, config.TOPIC_DLQ)]
    for attempt in range(1, retries + 1):
        admin = AIOKafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP)
        try:
            await admin.start()
            try:
                await admin.create_topics(topics)
            except TopicAlreadyExistsError:
                pass
            return
        except Exception as exc:  # noqa: BLE001
            log.warning("kafka not ready (%s/%s): %s", attempt, retries, exc)
            await asyncio.sleep(2)
        finally:
            await admin.close()
    raise RuntimeError("kafka never became ready")
