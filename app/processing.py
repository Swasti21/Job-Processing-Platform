"""Job state machine. Kafka-agnostic: takes a ``publisher`` so tests can run it without a broker.

Delivery is at-least-once, so every transition is guarded by the job's current status
(``UPDATE ... WHERE status = ...``). A redelivered message for a finished job is a no-op
and never double-counts a batch.
"""
import asyncio
import json
import logging
import random
import time
import uuid
from datetime import timedelta

from sqlalchemy import and_, case, func, insert, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from . import config
from .db import SessionLocal
from .extraction import PermanentError, extract_document
from .models import Batch, Document, ExtractionResult, Job, JobEvent

log = logging.getLogger("processing")


class TransientError(Exception):
    """Retryable failure (timeout, upstream hiccup, injected demo failure)."""


async def claim_job(session, job_id, worker_id):
    """Atomically move a job to PROCESSING. Returns (batch_id, attempts, max_attempts) or None.

    Claimable: PENDING, RETRYING, or PROCESSING with an expired lease (worker died mid-job).
    """
    stale = func.now() - timedelta(seconds=config.PROCESSING_LEASE_SECONDS)
    stmt = (
        update(Job)
        .where(Job.id == job_id,
               or_(Job.status.in_(("PENDING", "RETRYING")),
                   and_(Job.status == "PROCESSING", Job.started_at < stale)))
        .values(status="PROCESSING", attempts=Job.attempts + 1, started_at=func.now(),
                worker_id=worker_id, error=None)
        .returning(Job.batch_id, Job.attempts, Job.max_attempts)
    )
    row = (await session.execute(stmt)).first()
    return tuple(row) if row else None


async def _count_and_notify(session, batch_id, counter: str) -> None:
    """Bump a batch counter, close the batch if everything is terminal, NOTIFY the API."""
    col = getattr(Batch, counter)
    other = Batch.failed_count if counter == "completed_count" else Batch.completed_count
    stmt = (
        update(Batch)
        .where(Batch.id == batch_id)
        .values({
            counter: col + 1,
            "status": case(
                (col + 1 + other >= Batch.total_docs, "DONE"),
                else_=Batch.status,
            ),
            "completed_at": case(
                (and_(col + 1 + other >= Batch.total_docs, Batch.status != "DONE"), func.now()),
                else_=Batch.completed_at,
            ),
        })
        .returning(Batch.status, Batch.total_docs, Batch.completed_count, Batch.failed_count)
    )
    b = (await session.execute(stmt)).one()
    payload = json.dumps({"batch_id": str(batch_id), "status": b.status, "total": b.total_docs,
                          "completed": b.completed_count, "failed": b.failed_count})
    # pg_notify is transactional: listeners hear it only after this transaction commits.
    await session.execute(select(func.pg_notify("batch_progress", payload)))


async def complete_job(session_factory, job_id, batch_id, result: dict) -> bool:
    async with session_factory() as s, s.begin():
        done = await s.execute(
            update(Job).where(Job.id == job_id, Job.status == "PROCESSING")
            .values(status="COMPLETED", finished_at=func.now()).returning(Job.id))
        if done.first() is None:  # someone else already finished it
            return False
        stmt = pg_insert(ExtractionResult).values(
            job_id=job_id, fields=result["fields"], confidence=result["confidence"])
        await s.execute(stmt.on_conflict_do_update(
            index_elements=["job_id"],
            set_={"fields": stmt.excluded.fields, "confidence": stmt.excluded.confidence}))
        await s.execute(insert(JobEvent).values(job_id=job_id, event="COMPLETED"))
        await _count_and_notify(s, batch_id, "completed_count")
    return True


async def fail_job(session_factory, publisher, job_id, batch_id, attempts, max_attempts,
                   error: str, permanent: bool) -> str:
    terminal = permanent or attempts >= max_attempts
    async with session_factory() as s, s.begin():
        new_status = "FAILED" if terminal else "RETRYING"
        values = {"status": new_status, "error": error[:2000]}
        if terminal:
            values["finished_at"] = func.now()
        upd = await s.execute(
            update(Job).where(Job.id == job_id, Job.status == "PROCESSING").values(**values).returning(Job.id))
        if upd.first() is None:
            return "skipped"
        await s.execute(insert(JobEvent).values(
            job_id=job_id, event="FAILED" if terminal else "RETRY_SCHEDULED", detail=error[:2000]))
        if terminal:
            await _count_and_notify(s, batch_id, "failed_count")

    # Publish only after the DB commit. If publishing raises, the worker crashes, Kafka redelivers
    # the original message, and the job (now RETRYING) is simply claimed again.
    if terminal:
        await publisher.send(config.TOPIC_DLQ, str(job_id),
                             {"job_id": str(job_id), "batch_id": str(batch_id), "attempts": attempts, "error": error})
        return "failed"
    backoff = config.RETRY_BACKOFF_SECONDS * (2 ** (attempts - 1))
    await publisher.send(config.TOPIC_RETRY, str(job_id),
                         {"job_id": str(job_id), "batch_id": str(batch_id), "attempt": attempts,
                          "not_before": time.time() + backoff})
    return "retry"


async def process_message(msg: dict, publisher, worker_id: str, session_factory=SessionLocal) -> str:
    """Returns one of: skipped | completed | retry | failed."""
    job_id = uuid.UUID(msg["job_id"])
    async with session_factory() as s, s.begin():
        claimed = await claim_job(s, job_id, worker_id)
    if claimed is None:
        return "skipped"
    batch_id, attempts, max_attempts = claimed

    try:
        async with session_factory() as s:
            row = (await s.execute(
                select(Document.data, Job.filename).join(Job, Job.id == Document.job_id)
                .where(Document.job_id == job_id))).first()
        if row is None:
            raise PermanentError("document row missing")
        if config.PROCESSING_DELAY_MS:
            await asyncio.sleep(config.PROCESSING_DELAY_MS / 1000)
        if config.FAILURE_RATE and random.random() < config.FAILURE_RATE:
            raise TransientError("simulated transient failure")
        result = await asyncio.to_thread(extract_document, row.data, row.filename)
    except PermanentError as exc:
        return await fail_job(session_factory, publisher, job_id, batch_id, attempts, max_attempts,
                              str(exc), permanent=True)
    except Exception as exc:  # noqa: BLE001  (anything else is treated as retryable)
        return await fail_job(session_factory, publisher, job_id, batch_id, attempts, max_attempts,
                              f"{type(exc).__name__}: {exc}", permanent=False)

    await complete_job(session_factory, job_id, batch_id, result)
    return "completed"
