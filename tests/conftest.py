import os
import typing

import pytest
from pytest_factoryboy import register
from sqlalchemy.engine import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from .factories import EventFactory
from .factories import TaskFactory
from .factories import WorkerFactory
from .session import Session
from bq.config import normalize_database_url
from bq.db.base import Base

register(TaskFactory)
register(WorkerFactory)
register(EventFactory)


@pytest.fixture
def db_url() -> str:
    return normalize_database_url(
        os.environ.get("TEST_DB_URL", "postgresql+psycopg://bq:@localhost/bq_test")
    )


@pytest.fixture
def engine(db_url: str) -> Engine:
    return create_engine(db_url)


@pytest.fixture
def db(engine: Engine) -> typing.Generator[Session, None, None]:
    Session.configure(bind=engine)
    Base.metadata.create_all(bind=engine)
    try:
        yield Session
    finally:
        Session.remove()
        Base.metadata.drop_all(bind=engine)


@pytest.fixture
async def async_engine(
    db_url: str, db: Session
) -> typing.AsyncGenerator[AsyncEngine, None]:
    engine = create_async_engine(db_url)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def async_db(
    async_engine: AsyncEngine,
) -> typing.AsyncGenerator[AsyncSession, None]:
    maker = async_sessionmaker(async_engine, expire_on_commit=False)
    async with maker() as session:
        yield session
