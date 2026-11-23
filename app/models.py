import datetime as dt
import uuid

from sqlalchemy import (BigInteger, DateTime, Float, ForeignKey, Index, Integer,
                        LargeBinary, String, Text, func)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Job lifecycle: PENDING -> PROCESSING -> COMPLETED
#                                      -> RETRYING -> PROCESSING ... -> FAILED (after max attempts / permanent error)
JOB_STATUSES = ("PENDING", "PROCESSING", "RETRYING", "COMPLETED", "FAILED")
TERMINAL_STATUSES = ("COMPLETED", "FAILED")


class Base(DeclarativeBase):
    pass


def _now_col():
    return mapped_column(DateTime(timezone=True), server_default=func.now())


class Batch(Base):
    """One upload / one run. Counters are updated atomically by workers."""
    __tablename__ = "batches"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(200))
    status: Mapped[str] = mapped_column(String(20), default="PROCESSING", server_default="PROCESSING")
    total_docs: Mapped[int] = mapped_column(Integer, default=0)
    completed_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    failed_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    created_at: Mapped[dt.datetime] = _now_col()
    completed_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Job(Base):
    """One document = one job = one Kafka message."""
    __tablename__ = "jobs"
    __table_args__ = (Index("ix_jobs_batch_status", "batch_id", "status"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("batches.id", ondelete="CASCADE"))
    filename: Mapped[str] = mapped_column(String(300))
    status: Mapped[str] = mapped_column(String(12), default="PENDING", server_default="PENDING")
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, default=3, server_default="3")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    worker_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[dt.datetime] = _now_col()
    started_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Document(Base):
    """Raw bytes live here so any worker can fetch them; Kafka messages carry only IDs."""
    __tablename__ = "documents"

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True)
    content_type: Mapped[str] = mapped_column(String(100), default="text/plain")
    data: Mapped[bytes] = mapped_column(LargeBinary)
    # Only set for synthetic batches; used to measure extraction accuracy.
    ground_truth: Mapped[dict | None] = mapped_column(JSONB, nullable=True)


class ExtractionResult(Base):
    __tablename__ = "extraction_results"

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True)
    fields: Mapped[dict] = mapped_column(JSONB)
    confidence: Mapped[float] = mapped_column(Float)
    extracted_at: Mapped[dt.datetime] = _now_col()


class JobEvent(Base):
    """Audit trail: COMPLETED / RETRY_SCHEDULED / FAILED, with the error text."""
    __tablename__ = "job_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), index=True)
    event: Mapped[str] = mapped_column(String(30))
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[dt.datetime] = _now_col()
