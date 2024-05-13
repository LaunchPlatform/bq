import os
import typing

import pytest
from pytest_factoryboy import register
from sqlalchemy.engine import create_engine
from sqlalchemy.engine import Engine

from .factories import TaskFactory
from .factories import WorkerFactory
from bq.db.base import Base
from bq.db.session import Session

register(TaskFactory)
register(WorkerFactory)


@pytest.fixture
def db_url() -> str:
    return os.environ.get("TEST_DB_URL", "postgresql://bq:@localhost/bq_test")


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
