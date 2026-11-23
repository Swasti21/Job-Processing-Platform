"""FastAPI app.   Run:  uvicorn app.api:app --reload"""
import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, insert, select

from . import config, evaluation
from .bus import KafkaPublisher, LocalPublisher, ensure_topics
from .db import SessionLocal, init_db
from .models import Batch, Document, ExtractionResult, Job, JobEvent
from .progress import ProgressHub, listen_forever
from .schemas import BatchOut, JobDetail, JobOut, SyntheticRequest
from .synthetic import generate_batch

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    app.state.hub = ProgressHub()
    background: list[asyncio.Task] = []
    if config.BUS == "local":
        from .local import start_local_workers
        app.state.publisher = LocalPublisher()
        background += start_local_workers(app.state.publisher)
    else:
        await ensure_topics()
        app.state.publisher = KafkaPublisher()
        await app.state.publisher.start()
    background.append(asyncio.create_task(listen_forever(app.state.hub)))
    yield
    for t in background:
        t.cancel()
    await asyncio.gather(*background, return_exceptions=True)
    await app.state.publisher.stop()


app = FastAPI(title="Distributed Job Processing Platform", lifespan=lifespan)


async def get_session():
    async with SessionLocal() as session:
        yield session


def get_publisher(request: Request):
    return request.app.state.publisher


def _batch_out(b: Batch) -> BatchOut:
    out = BatchOut.model_validate(b)
    out.progress = round((b.completed_count + b.failed_count) / b.total_docs, 4) if b.total_docs else 0.0
    return out


# ------------------------------------------------------------------ creating batches
async def _create_batch(session, name: str, docs: list[dict]) -> Batch:
    """docs: [{filename, content_type, data, ground_truth}]. One transaction, bulk inserts."""
    batch = Batch(name=name, total_docs=len(docs))
    session.add(batch)
    await session.flush()
    job_rows, doc_rows = [], []
    for d in docs:
        jid = uuid.uuid4()
        job_rows.append({"id": jid, "batch_id": batch.id, "filename": d["filename"],
                         "status": "PENDING", "attempts": 0, "max_attempts": config.MAX_ATTEMPTS})
        doc_rows.append({"job_id": jid, "content_type": d.get("content_type", "text/plain"),
                         "data": d["data"], "ground_truth": d.get("ground_truth")})
    for i in range(0, len(job_rows), 1000):  # chunk to stay under the driver's parameter limit
        await session.execute(insert(Job), job_rows[i:i + 1000])
        await session.execute(insert(Document), doc_rows[i:i + 1000])
    await session.commit()
    return batch


async def _publish_jobs(publisher, items: list[tuple[str, str]]) -> None:
    """items: [(job_id, batch_id)]. Key = job_id so a batch spreads across all partitions."""
    msgs = [(jid, {"job_id": jid, "batch_id": bid, "attempt": 0}) for jid, bid in items]
    for i in range(0, len(msgs), 1000):
        await publisher.send_many(config.TOPIC_SUBMITTED, msgs[i:i + 1000])


async def _publish_batch(publisher, batch_id: uuid.UUID) -> None:
    async with SessionLocal() as s:
        ids = (await s.execute(select(Job.id).where(Job.batch_id == batch_id, Job.status == "PENDING"))).scalars().all()
    await _publish_jobs(publisher, [(str(j), str(batch_id)) for j in ids])


@app.post("/api/batches/upload", response_model=BatchOut, status_code=201)
async def upload_batch(files: list[UploadFile], background: BackgroundTasks,
                       session=Depends(get_session), publisher=Depends(get_publisher)):
    if not files:
        raise HTTPException(400, "no files")
    docs = []
    for f in files:
        data = await f.read()
        if len(data) > config.MAX_UPLOAD_BYTES:
            raise HTTPException(413, f"{f.filename} is larger than {config.MAX_UPLOAD_BYTES} bytes")
        docs.append({"filename": f.filename or "upload", "content_type": f.content_type or "application/octet-stream",
                     "data": data})
    batch = await _create_batch(session, f"upload of {len(docs)} files", docs)
    background.add_task(_publish_batch, publisher, batch.id)
    return _batch_out(batch)


@app.post("/api/batches/synthetic", response_model=BatchOut, status_code=201)
async def synthetic_batch(req: SyntheticRequest, background: BackgroundTasks,
                          session=Depends(get_session), publisher=Depends(get_publisher)):
    docs = [{"filename": fn, "data": text.encode(), "ground_truth": truth}
            for fn, text, truth in generate_batch(req.count, req.seed, req.corrupt_rate)]
    batch = await _create_batch(session, req.name or f"synthetic x{req.count}", docs)
    background.add_task(_publish_batch, publisher, batch.id)
    return _batch_out(batch)


@app.post("/api/batches/{batch_id}/requeue")
async def requeue(batch_id: uuid.UUID, publisher=Depends(get_publisher), session=Depends(get_session)):
    """Re-publish jobs that are still PENDING (e.g. the API crashed between DB commit and Kafka send)."""
    if await session.get(Batch, batch_id) is None:
        raise HTTPException(404, "batch not found")
    await _publish_batch(publisher, batch_id)
    return {"requeued": True}


# ------------------------------------------------------------------ reading
@app.get("/api/batches", response_model=list[BatchOut])
async def list_batches(limit: int = Query(20, le=100), session=Depends(get_session)):
    rows = (await session.execute(select(Batch).order_by(Batch.created_at.desc()).limit(limit))).scalars().all()
    return [_batch_out(b) for b in rows]


@app.get("/api/batches/{batch_id}", response_model=BatchOut)
async def get_batch(batch_id: uuid.UUID, session=Depends(get_session)):
    b = await session.get(Batch, batch_id)
    if b is None:
        raise HTTPException(404, "batch not found")
    return _batch_out(b)


@app.get("/api/batches/{batch_id}/jobs", response_model=list[JobOut])
async def list_jobs(batch_id: uuid.UUID, status: str | None = None, limit: int = Query(100, le=1000),
                    offset: int = 0, session=Depends(get_session)):
    q = select(Job).where(Job.batch_id == batch_id).order_by(Job.filename).limit(limit).offset(offset)
    if status:
        q = q.where(Job.status == status.upper())
    return (await session.execute(q)).scalars().all()


@app.get("/api/batches/{batch_id}/status-counts")
async def status_counts(batch_id: uuid.UUID, session=Depends(get_session)):
    rows = (await session.execute(
        select(Job.status, func.count()).where(Job.batch_id == batch_id).group_by(Job.status))).all()
    return {status: n for status, n in rows}


@app.get("/api/jobs/{job_id}", response_model=JobDetail)
async def get_job(job_id: uuid.UUID, session=Depends(get_session)):
    job = await session.get(Job, job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    res = await session.get(ExtractionResult, job_id)
    events = (await session.execute(
        select(JobEvent).where(JobEvent.job_id == job_id).order_by(JobEvent.id))).scalars().all()
    out = JobDetail.model_validate(job)
    out.fields = res.fields if res else None
    out.confidence = res.confidence if res else None
    out.events = [{"event": e.event, "detail": e.detail, "at": e.created_at.isoformat()} for e in events]
    return out


@app.get("/api/batches/{batch_id}/accuracy")
async def batch_accuracy(batch_id: uuid.UUID, session=Depends(get_session)):
    """Extraction accuracy vs. ground truth. Only meaningful for synthetic batches."""
    rows = (await session.execute(
        select(ExtractionResult.fields, Document.ground_truth)
        .join(Document, Document.job_id == ExtractionResult.job_id)
        .join(Job, Job.id == ExtractionResult.job_id)
        .where(Job.batch_id == batch_id, Document.ground_truth.is_not(None)))).all()
    if not rows:
        raise HTTPException(404, "no labelled results for this batch yet")
    return evaluation.score((fields, truth) for fields, truth in rows)


@app.get("/api/batches/{batch_id}/results")
async def batch_results(batch_id: uuid.UUID, limit: int = Query(500, le=5000), offset: int = 0,
                        session=Depends(get_session)):
    rows = (await session.execute(
        select(Job.filename, ExtractionResult.fields, ExtractionResult.confidence)
        .join(ExtractionResult, ExtractionResult.job_id == Job.id)
        .where(Job.batch_id == batch_id).order_by(Job.filename).limit(limit).offset(offset))).all()
    return [{"filename": f, "fields": fl, "confidence": c} for f, fl, c in rows]


@app.get("/health")
async def health(session=Depends(get_session)):
    await session.execute(select(1))
    return {"status": "ok", "bus": config.BUS}


# ------------------------------------------------------------------ websocket
@app.websocket("/ws/batches/{batch_id}")
async def ws_batch(ws: WebSocket, batch_id: uuid.UUID):
    hub: ProgressHub = ws.app.state.hub
    key = str(batch_id)
    await hub.connect(key, ws)
    try:
        async with SessionLocal() as s:  # initial snapshot so late joiners see current state
            b = await s.get(Batch, batch_id)
        if b is None:
            await ws.close(code=4404)
            return
        await ws.send_json({"batch_id": key, "status": b.status, "total": b.total_docs,
                            "completed": b.completed_count, "failed": b.failed_count})
        while True:
            await ws.receive_text()  # we only push; this just detects disconnects
    except WebSocketDisconnect:
        pass
    finally:
        hub.disconnect(key, ws)


_static = Path(__file__).resolve().parent.parent / "static"
if _static.exists():
    app.mount("/", StaticFiles(directory=_static, html=True), name="static")
