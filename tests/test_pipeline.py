"""Integration tests against a real Postgres (no Kafka: uses the in-memory LocalPublisher).

    TEST_DATABASE_URL=postgresql+asyncpg://jobs:jobs@localhost:5432/jobs_test pytest
"""
import asyncio
import json
import uuid

import asyncpg
import httpx
import pytest
from sqlalchemy import select, text, update

from app import config, evaluation
from app.api import _create_batch, _publish_batch, app
from app.bus import LocalPublisher
from app.db import SessionLocal, asyncpg_dsn
from app.local import drain
from app.models import Batch, Document, ExtractionResult, Job, JobEvent
from app.processing import _count_and_notify, process_message
from app.synthetic import generate_batch
from tests.conftest import needs_db

pytestmark = needs_db


async def make_batch(n=10, corrupt_rate=0.0, seed=1):
    docs = [{"filename": fn, "data": t.encode(), "ground_truth": truth}
            for fn, t, truth in generate_batch(n, seed, corrupt_rate)]
    async with SessionLocal() as s:
        return await _create_batch(s, "test", docs)


async def get_job(job_id):
    async with SessionLocal() as s:
        return await s.get(Job, job_id)


async def get_batch(batch_id):
    async with SessionLocal() as s:
        return await s.get(Batch, batch_id)


async def first_job_id(batch_id):
    async with SessionLocal() as s:
        return (await s.execute(select(Job.id).where(Job.batch_id == batch_id))).scalars().first()


async def test_happy_path_completes_batch_and_accuracy_is_sane(db):
    batch = await make_batch(25)
    pub = LocalPublisher()
    await _publish_batch(pub, batch.id)
    assert len(pub.sent) == 25 and all(t == config.TOPIC_SUBMITTED for t, _, _ in pub.sent)
    await drain(pub)

    b = await get_batch(batch.id)
    assert (b.status, b.completed_count, b.failed_count) == ("DONE", 25, 0)
    assert b.completed_at is not None
    async with SessionLocal() as s:
        rows = (await s.execute(
            select(ExtractionResult.fields, Document.ground_truth)
            .join(Document, Document.job_id == ExtractionResult.job_id))).all()
    assert len(rows) == 25
    assert evaluation.score(rows)["field_accuracy"] > 0.8


async def test_transient_failure_is_retried_then_succeeds(db, monkeypatch):
    batch = await make_batch(1)
    jid = await first_job_id(batch.id)
    pub = LocalPublisher()
    msg = {"job_id": str(jid), "batch_id": str(batch.id)}

    monkeypatch.setattr(config, "FAILURE_RATE", 1.0)
    assert await process_message(msg, pub, "w1") == "retry"
    job = await get_job(jid)
    assert job.status == "RETRYING" and job.attempts == 1 and "simulated" in job.error
    topic, _, payload = pub.sent[-1]
    assert topic == config.TOPIC_RETRY and "not_before" in payload

    monkeypatch.setattr(config, "FAILURE_RATE", 0.0)
    assert await process_message(payload, pub, "w2") == "completed"
    job = await get_job(jid)
    assert (job.status, job.attempts, job.worker_id) == ("COMPLETED", 2, "w2")
    async with SessionLocal() as s:
        events = (await s.execute(select(JobEvent.event).where(JobEvent.job_id == jid).order_by(JobEvent.id))).scalars().all()
    assert events == ["RETRY_SCHEDULED", "COMPLETED"]


async def test_gives_up_after_max_attempts_and_goes_to_dlq(db, monkeypatch):
    batch = await make_batch(1)
    jid = await first_job_id(batch.id)
    pub = LocalPublisher()
    await _publish_batch(pub, batch.id)
    monkeypatch.setattr(config, "FAILURE_RATE", 1.0)
    await drain(pub)

    job = await get_job(jid)
    assert (job.status, job.attempts) == ("FAILED", config.MAX_ATTEMPTS)
    assert [t for t, _, _ in pub.sent].count(config.TOPIC_RETRY) == config.MAX_ATTEMPTS - 1
    assert pub.sent[-1][0] == config.TOPIC_DLQ
    b = await get_batch(batch.id)
    assert (b.status, b.completed_count, b.failed_count) == ("DONE", 0, 1)


async def test_corrupt_document_fails_immediately_without_retry(db):
    batch = await make_batch(3, corrupt_rate=1.0)
    pub = LocalPublisher()
    await _publish_batch(pub, batch.id)
    await drain(pub)
    async with SessionLocal() as s:
        jobs = (await s.execute(select(Job).where(Job.batch_id == batch.id))).scalars().all()
    assert all(j.status == "FAILED" and j.attempts == 1 for j in jobs)
    assert not any(t == config.TOPIC_RETRY for t, _, _ in pub.sent)
    assert (await get_batch(batch.id)).failed_count == 3


async def test_duplicate_delivery_is_idempotent(db):
    batch = await make_batch(2)
    jid = await first_job_id(batch.id)
    pub = LocalPublisher()
    msg = {"job_id": str(jid), "batch_id": str(batch.id)}
    assert await process_message(msg, pub, "w1") == "completed"
    assert await process_message(msg, pub, "w2") == "skipped"      # Kafka redelivery
    assert (await get_batch(batch.id)).completed_count == 1         # not double counted


async def test_concurrent_workers_cannot_both_process_the_same_job(db, monkeypatch):
    monkeypatch.setattr(config, "PROCESSING_DELAY_MS", 100.0)
    batch = await make_batch(1)
    jid = await first_job_id(batch.id)
    msg = {"job_id": str(jid), "batch_id": str(batch.id)}
    results = await asyncio.gather(*(process_message(msg, LocalPublisher(), f"w{i}") for i in range(5)))
    assert sorted(results) == ["completed"] + ["skipped"] * 4
    assert (await get_batch(batch.id)).completed_count == 1


async def test_job_abandoned_by_dead_worker_is_reclaimed_after_lease(db, monkeypatch):
    batch = await make_batch(1)
    jid = await first_job_id(batch.id)
    msg = {"job_id": str(jid), "batch_id": str(batch.id)}
    async with SessionLocal() as s, s.begin():  # simulate a worker that crashed mid-job
        await s.execute(update(Job).where(Job.id == jid).values(
            status="PROCESSING", attempts=1, started_at=text("now() - interval '1 hour'")))
    assert await process_message(msg, LocalPublisher(), "w2") == "completed"
    # ...but a fresh PROCESSING job (live worker) must not be stolen
    batch2 = await make_batch(1, seed=2)
    jid2 = await first_job_id(batch2.id)
    async with SessionLocal() as s, s.begin():
        await s.execute(update(Job).where(Job.id == jid2).values(status="PROCESSING", started_at=text("now()")))
    assert await process_message({"job_id": str(jid2), "batch_id": str(batch2.id)}, LocalPublisher(), "w3") == "skipped"


async def test_progress_is_notified_over_postgres(db):
    batch = await make_batch(5)
    received = []
    conn = await asyncpg.connect(asyncpg_dsn())
    await conn.add_listener("batch_progress", lambda _c, _p, _ch, payload: received.append(json.loads(payload)))
    try:
        pub = LocalPublisher()
        await _publish_batch(pub, batch.id)
        await drain(pub)
        await asyncio.sleep(0.3)
    finally:
        await conn.close()
    assert len(received) == 5
    assert received[-1] == {"batch_id": str(batch.id), "status": "DONE", "total": 5, "completed": 5, "failed": 0}
    assert [r["completed"] for r in received] == [1, 2, 3, 4, 5]


# ----------------------------------------------------------------------------- HTTP API
@pytest.fixture
async def client(db):
    app.state.publisher = LocalPublisher()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        yield c


async def test_api_synthetic_batch_end_to_end(client):
    r = await client.post("/api/batches/synthetic", json={"count": 30, "corrupt_rate": 0.0, "seed": 3})
    assert r.status_code == 201
    bid = r.json()["id"]
    assert r.json()["status"] == "PROCESSING" and r.json()["total_docs"] == 30

    await drain(app.state.publisher)

    b = (await client.get(f"/api/batches/{bid}")).json()
    assert (b["status"], b["completed_count"], b["progress"]) == ("DONE", 30, 1.0)
    assert (await client.get(f"/api/batches/{bid}/status-counts")).json() == {"COMPLETED": 30}
    jobs = (await client.get(f"/api/batches/{bid}/jobs", params={"limit": 5})).json()
    assert len(jobs) == 5 and jobs[0]["status"] == "COMPLETED"
    detail = (await client.get(f"/api/jobs/{jobs[0]['id']}")).json()
    assert detail["fields"]["invoice_number"].startswith("INV-") and detail["events"][0]["event"] == "COMPLETED"
    acc = (await client.get(f"/api/batches/{bid}/accuracy")).json()
    assert acc["documents"] == 30 and 0.8 < acc["field_accuracy"] <= 1.0
    results = (await client.get(f"/api/batches/{bid}/results", params={"limit": 3})).json()
    assert len(results) == 3 and "total" in results[0]["fields"]
    assert any(b["id"] == bid for b in (await client.get("/api/batches")).json())


async def test_api_upload_and_requeue(client):
    files = [("files", (f"inv{i}.txt", f"Acme Corp\nInvoice No: INV-{i}00\nInvoice Date: 2026-01-0{i}\nTotal Amount: ${i}00.00\n".encode(), "text/plain"))
             for i in range(1, 4)]
    r = await client.post("/api/batches/upload", files=files)
    assert r.status_code == 201 and r.json()["total_docs"] == 3
    bid = r.json()["id"]
    pub = app.state.publisher
    pub.queue = asyncio.Queue()           # pretend the original publish was lost
    assert (await client.post(f"/api/batches/{bid}/requeue")).status_code == 200
    assert pub.queue.qsize() == 3
    await drain(pub)
    assert (await client.get(f"/api/batches/{bid}")).json()["status"] == "DONE"


async def test_api_404s_and_validation(client):
    missing = uuid.uuid4()
    assert (await client.get(f"/api/batches/{missing}")).status_code == 404
    assert (await client.get(f"/api/jobs/{missing}")).status_code == 404
    assert (await client.get(f"/api/batches/{missing}/accuracy")).status_code == 404
    assert (await client.post("/api/batches/synthetic", json={"count": 0})).status_code == 422
    assert (await client.get("/health")).json()["status"] == "ok"


async def test_atomic_count_and_notify_concurrency(db):
    batch = await make_batch(50)

    async def call_count(counter):
        async with SessionLocal() as s, s.begin():
            await _count_and_notify(s, batch.id, counter)

    await asyncio.gather(*(call_count("completed_count") for _ in range(40)),
                         *(call_count("failed_count") for _ in range(10)))

    b = await get_batch(batch.id)
    assert (b.status, b.completed_count, b.failed_count) == ("DONE", 40, 10)
    assert b.completed_at is not None
