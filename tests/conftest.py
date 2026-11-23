import os
import sys
from pathlib import Path

# Must run before any `app` import: the SQLAlchemy engine reads DATABASE_URL at import time.
TEST_DB = os.getenv("TEST_DATABASE_URL")
if TEST_DB:
    os.environ["DATABASE_URL"] = TEST_DB
os.environ["BUS"] = "local"
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402

needs_db = pytest.mark.skipif(not TEST_DB, reason="set TEST_DATABASE_URL (postgresql+asyncpg://...) to run DB tests")


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def _schema():
    from app.db import init_db
    await init_db()


@pytest_asyncio.fixture(loop_scope="session")
async def db(_schema):
    """Clean tables before every DB test."""
    from sqlalchemy import text
    from app.db import SessionLocal
    async with SessionLocal() as s, s.begin():
        await s.execute(text("TRUNCATE batches, jobs, documents, extraction_results, job_events RESTART IDENTITY CASCADE"))
    yield


@pytest.fixture(autouse=True)
def _reset_config(monkeypatch):
    from app import config
    monkeypatch.setattr(config, "FAILURE_RATE", 0.0)
    monkeypatch.setattr(config, "PROCESSING_DELAY_MS", 0.0)
    monkeypatch.setattr(config, "RETRY_BACKOFF_SECONDS", 0.0)
