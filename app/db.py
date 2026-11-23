import asyncio
import logging

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from . import config
from .models import Base

log = logging.getLogger("db")

engine = create_async_engine(config.DATABASE_URL, pool_size=20, max_overflow=10, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def init_db(retries: int = 30) -> None:
    """Create tables if missing. Retries because Postgres may still be starting."""
    for attempt in range(1, retries + 1):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            return
        except Exception as exc:  # noqa: BLE001
            log.warning("database not ready (%s/%s): %s", attempt, retries, exc)
            await asyncio.sleep(1)
    raise RuntimeError("database never became ready")


def asyncpg_dsn() -> str:
    """Plain DSN for the raw asyncpg connection used by LISTEN/NOTIFY."""
    return config.DATABASE_URL.replace("+asyncpg", "")
