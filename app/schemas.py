import datetime as dt
import uuid

from pydantic import BaseModel, ConfigDict, Field


class BatchOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    name: str
    status: str
    total_docs: int
    completed_count: int
    failed_count: int
    created_at: dt.datetime
    completed_at: dt.datetime | None = None
    progress: float = 0.0  # 0..1, finished (completed + failed) / total


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    batch_id: uuid.UUID
    filename: str
    status: str
    attempts: int
    max_attempts: int
    error: str | None = None
    worker_id: str | None = None
    started_at: dt.datetime | None = None
    finished_at: dt.datetime | None = None


class JobDetail(JobOut):
    fields: dict | None = None
    confidence: float | None = None
    events: list[dict] = []


class SyntheticRequest(BaseModel):
    count: int = Field(500, ge=1, le=20000)
    corrupt_rate: float = Field(0.02, ge=0, le=1)
    seed: int | None = None
    name: str | None = None
