"""All settings come from environment variables so Docker Compose can override them.

Values are read as module attributes at call time (``config.X``), which lets tests
monkeypatch them.
"""
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://jobs:jobs@localhost:5432/jobs")

# "kafka" = real Kafka (docker compose). "local" = in-process queue, no Kafka needed.
BUS = os.getenv("BUS", "kafka")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")

TOPIC_SUBMITTED = "documents.submitted"
TOPIC_RETRY = "documents.retry"
TOPIC_DLQ = "documents.dlq"
# Max useful parallelism = number of partitions (one consumer per partition).
TOPIC_PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "6"))
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "doc-workers")

MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))
RETRY_BACKOFF_SECONDS = float(os.getenv("RETRY_BACKOFF_SECONDS", "2"))  # doubles each attempt
# A job stuck in PROCESSING longer than this is assumed to belong to a dead worker.
PROCESSING_LEASE_SECONDS = float(os.getenv("PROCESSING_LEASE_SECONDS", "30"))

WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "16"))
# Demo knobs: slow each job down a little so progress is visible; inject random transient failures.
PROCESSING_DELAY_MS = float(os.getenv("PROCESSING_DELAY_MS", "0"))
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0"))

MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(5 * 1024 * 1024)))
