# Distributed Job Processing Platform: one-day plan

**Goal:** upload a batch of documents (5,000+), process them asynchronously across horizontally scalable workers, retry failures, show live progress, and measure extraction accuracy against labelled test cases.

**Cut for day one (listed under "Later"):** auth, real OCR/ML models, Alembic migrations, object storage, Kubernetes, metrics dashboards.

---

## 1. Tools

| Need | Tool | Why |
|---|---|---|
| API + WebSocket | FastAPI + uvicorn | async, WebSockets built in, auto docs at `/docs` |
| Event streaming | Apache Kafka (KRaft, single node) via `aiokafka` | the core of the project; no ZooKeeper |
| Database | PostgreSQL 16, SQLAlchemy 2 (async) + asyncpg | job state, results, audit trail |
| Live progress | Postgres `LISTEN/NOTIFY` | workers notify, API pushes to WebSockets; no extra infrastructure |
| Extraction | regex + normalisers (`pypdf` for PDFs) | simple and measurable; swap for OCR/LLM later |
| Packaging | Docker + Docker Compose | one command starts everything |
| Tests | pytest, pytest-asyncio, httpx | real Postgres, in-memory bus |

## 2. Flow

```
 client ──POST /api/batches/upload|synthetic──▶ API ──┬─▶ Postgres: batch + jobs (PENDING) + documents
   ▲                                                  └─▶ Kafka  topic documents.submitted  (key = job_id)
   │                                                                 │
   │ WebSocket /ws/batches/{id}                          consumer group "doc-workers"
   │                                                     ┌───────────┼───────────┐
   │                                                  worker 1    worker 2    worker N      (scale with --scale)
   │                                                     │ 1. claim job (PENDING→PROCESSING, atomic UPDATE)
   │                                                     │ 2. load document, extract fields
   │                                                     │ 3a. success → result row, COMPLETED, batch counter +1
   │                                                     │ 3b. transient error → RETRYING, publish documents.retry (back-off)
   │                                                     │ 3c. permanent error / attempts exhausted → FAILED, publish documents.dlq
   │                                                     │ 4. pg_notify('batch_progress', counts)   ← same transaction
   │                                                     ▼
   └──────────── API (LISTEN batch_progress) ◀──── Postgres
```

**Delivery semantics:** at-least-once. Offsets are committed only after the database work finishes. Every state change is `UPDATE ... WHERE status = <expected>`, so duplicates and redeliveries are harmless (tested).

**Job states:** `PENDING → PROCESSING → COMPLETED`, or `PROCESSING → RETRYING → PROCESSING … → FAILED`.

**Kafka design**
- `documents.submitted`, `documents.retry`, `documents.dlq`: 6 partitions each, so up to 6 workers run in parallel.
- Message key = `job_id` (not `batch_id`, which would put a whole batch on one partition and kill parallelism).
- Messages carry only IDs. The bytes live in Postgres, so messages stay tiny.
- Retry back-off: the retry message carries `not_before`; the worker waits until it is due. Back-off doubles per attempt.

## 3. Schema

```
batches            id uuid PK | name | status (PROCESSING/DONE) | total_docs | completed_count | failed_count
                   | created_at | completed_at

jobs               id uuid PK | batch_id FK→batches | filename | status | attempts | max_attempts | error
                   | worker_id | created_at | started_at | finished_at
                   INDEX (batch_id, status)

documents          job_id PK/FK→jobs | content_type | data bytea | ground_truth jsonb (synthetic only)

extraction_results job_id PK/FK→jobs | fields jsonb | confidence float | extracted_at

job_events         id bigserial PK | job_id FK→jobs (indexed) | event (COMPLETED/RETRY_SCHEDULED/FAILED)
                   | detail | created_at
```

The exact DDL is in `db/schema.sql` (generated from `app/models.py`).

Extracted `fields`: `invoice_number, invoice_date, due_date, vendor, total, currency`.

## 4. API

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/batches/upload` | multipart `files[]` (.txt / .pdf) |
| POST | `/api/batches/synthetic` | `{count, corrupt_rate, seed}`: generate labelled test docs |
| GET | `/api/batches`, `/api/batches/{id}` | list / one batch with progress |
| GET | `/api/batches/{id}/jobs?status=` | paged job list |
| GET | `/api/batches/{id}/status-counts` | group-by status |
| GET | `/api/batches/{id}/results` | extracted fields |
| GET | `/api/batches/{id}/accuracy` | accuracy vs. ground truth |
| GET | `/api/jobs/{id}` | job + result + event history |
| POST | `/api/batches/{id}/requeue` | re-publish PENDING jobs |
| WS | `/ws/batches/{id}` | live `{total, completed, failed, status}` |

## 5. Timeline (about 8 hours if you build from scratch)

| Hours | Step | Done when |
|---|---|---|
| 0.5 | Compose file: Postgres + Kafka; models + `create_all` | `docker compose up postgres kafka` is healthy, tables exist |
| 1.0 | Extraction + synthetic data + `scripts/evaluate.py` | accuracy script prints per-field numbers |
| 1.5 | API: create batch, bulk insert, publish to Kafka | `POST /synthetic` returns a batch, topic has 5,000 messages |
| 1.5 | Worker: consume, claim, extract, write result | batch reaches DONE with one worker |
| 1.0 | Retries + DLQ + idempotency | injected failures recover; corrupt docs land in the DLQ |
| 1.0 | WebSocket progress via LISTEN/NOTIFY + simple UI | progress bar moves live |
| 1.0 | Tests, Dockerfile, README, `--scale worker=3` run | `pytest` green, all services start with one command |

**This repo already contains all of it.** Your day becomes: run it (30 min), read each module in the order above (2 h), run the load test and failure drills (1 h), swap in real documents for the accuracy check (2 h), and be ready to explain every design choice (1 h).

## 6. Later

- Replace regex with OCR (Tesseract) or an LLM extractor behind `extract_document`.
- Transactional outbox so DB commit and Kafka publish cannot diverge (today `/requeue` is the manual fix).
- Sweeper that re-queues jobs stuck in `PROCESSING` after a worker crash. Today a crashed job is recovered only if Kafka redelivers its message after the lease has expired; an earlier redelivery is skipped and the job stays stuck.
- Per-batch counter hot-row: every finishing job updates the same `batches` row. Fine at this scale; shard the counters or derive them from `jobs` if you push far beyond it.
- Alembic migrations, auth, object storage for large files, Prometheus/Grafana, Kubernetes.
