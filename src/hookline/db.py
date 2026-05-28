from collections.abc import AsyncIterator
from datetime import UTC, datetime

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool, StaticPool

from hookline.models import Base


def make_engine(
    url: str, *, pool_size: int = 5, max_overflow: int = 5, null_pool: bool = False
) -> AsyncEngine:
    if url.startswith("sqlite"):
        # One shared in-memory database for tests and quick demos.
        return create_async_engine(
            url, connect_args={"check_same_thread": False}, poolclass=StaticPool
        )
    if null_pool:
        return create_async_engine(url, poolclass=NullPool)
    return create_async_engine(
        url, pool_pre_ping=True, pool_size=pool_size, max_overflow=max_overflow
    )


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def create_all(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


def is_postgres(session: AsyncSession) -> bool:
    return session.get_bind().dialect.name == "postgresql"


async def db_now(session: AsyncSession) -> datetime:
    """The database's clock, not this machine's.

    Every scheduling decision (when an attempt is due, when a lease runs out, when a circuit
    closes) compares against this, so workers on hosts with skewed clocks still agree.
    """
    if is_postgres(session):
        value: object = (await session.execute(text("SELECT now()"))).scalar_one()
        assert isinstance(value, datetime)
        return value.astimezone(UTC)
    return datetime.now(UTC)


async def session_scope(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    async with sessionmaker() as session:
        yield session


__all__ = ["create_all", "db_now", "make_engine", "make_sessionmaker", "select"]
