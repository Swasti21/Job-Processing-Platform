# Distributed Job Processing Platform

Asynchronous document processing: upload a batch → Kafka → scalable workers → PostgreSQL, with retries, a dead-letter queue, live WebSocket progress and an accuracy check. See **PLAN.md** for the design, schema and flow.

## Run it (Docker)

```bash
docker compose up --build            # postgres, kafka, api, 2 workers
# open http://localhost:8000         (UI)   and   http://localhost:8000/docs   (API)
```

Click **Run 5,000 synthetic docs** in the UI, or from a terminal:

```bash
pip install httpx && python scripts/load_test.py --count 5000
```

Scale workers (6 partitions → up to 6 useful consumers):

```bash
docker compose up -d --scale worker=4
```

Failure drills:

```bash
docker compose kill worker           # jobs pause; nothing is lost
docker compose up -d worker          # they resume and the batch finishes
docker compose logs -f worker        # watch claims, retries
```

The compose file turns on demo knobs (`PROCESSING_DELAY_MS=100`, `FAILURE_RATE=0.03`) so progress is visible and retries actually happen. Set both to `0` for raw speed.

## Run without Kafka (fallback)

```bash
docker compose up -d postgres
pip install -r requirements.txt
DATABASE_URL=postgresql+asyncpg://jobs:jobs@localhost:5432/jobs BUS=local uvicorn app.api:app --reload
```

`BUS=local` runs the workers inside the API process on an in-memory queue. Same database, same state machine, same retries.

## Tests and accuracy

```bash
pip install -r requirements-dev.txt
docker compose up -d postgres
docker compose exec postgres createdb -U jobs jobs_test
TEST_DATABASE_URL=postgresql+asyncpg://jobs:jobs@localhost:5432/jobs_test pytest
python scripts/evaluate.py -v        # no database needed
```

## Layout

```
app/api.py         FastAPI routes + WebSocket          app/processing.py  job state machine (claim/complete/retry/fail)
app/worker.py      Kafka consumer loop                 app/bus.py         Kafka + in-memory publishers, topic creation
app/extraction.py  regex field extraction              app/progress.py    LISTEN/NOTIFY → WebSocket hub
app/synthetic.py   labelled test-doc generator         app/local.py       no-Kafka worker runner
app/evaluation.py  accuracy scoring                    app/models.py      SQLAlchemy tables (db/schema.sql is generated)
```

## What was verified, and what was not

Verified (in a sandbox with a real PostgreSQL 16, no Docker or Kafka available there):
- 23 tests pass: extraction, state machine, retries, DLQ, duplicate delivery, concurrent claims, lease reclaim, NOTIFY, every API route.
- A 5,000-document batch ran end to end through the API with the in-memory bus (`BUS=local`), progress streamed over a real WebSocket, and ~150 injected transient failures all recovered through retries.
- aiokafka call signatures were checked against the installed library.

**Not verified: the Docker Compose stack and the real Kafka path.** Treat the first `docker compose up` as your integration test. If Kafka misbehaves, check `docker compose logs kafka`, and use the `BUS=local` fallback above to keep working.

## Before you put numbers on your resume

- `scripts/evaluate.py` scores 40 **synthetic** cases at about 99% field accuracy. That is not a real-world number: the documents come from this project's own generator, and the only misses are the OCR typos the generator injects on purpose.
- Get your own figure: put 30–40 real or realistic invoices in a folder with a `labels.json` (format in the script's docstring) and run `python scripts/evaluate.py --dir yourfolder`. Quote exactly what it prints.
- The same goes for throughput: quote what `scripts/load_test.py` reports on your machine with Kafka running.

## Known limitations

- A job whose worker dies mid-processing stays `PROCESSING`. It is recovered only if Kafka redelivers its message after the 30 s lease has expired; if the redelivery comes sooner, the job is skipped and stays stuck (a sweeper that re-queues stale jobs is the fix, see PLAN.md). The lease is `PROCESSING_LEASE_SECONDS`.
- Publishing to Kafka happens after the DB commit; if the API dies in between, use `POST /api/batches/{id}/requeue`.
- Dates like `03/04/2026` are read as DD/MM/YYYY.
