import functools
import typing

from dependency_injector import containers
from dependency_injector import providers
from sqlalchemy import create_engine
from sqlalchemy import Engine
from sqlalchemy.orm import Session as DBSession
from sqlalchemy.pool import SingletonThreadPool

from .config import Config
from .db.session import SessionMaker
from .services.dispatch import DispatchService
from .services.worker import WorkerService


def make_db_engine(config: Config) -> Engine:
    return create_engine(str(config.DATABASE_URL), poolclass=SingletonThreadPool)


def make_session_factory(engine: Engine) -> typing.Callable:
    return functools.partial(SessionMaker, bind=engine)


def make_session(factory: typing.Callable) -> DBSession:
    return factory()


def make_dispatch_service(session: DBSession) -> DispatchService:
    return DispatchService(session)


def make_worker_service(session: DBSession) -> WorkerService:
    return WorkerService(session)


class Container(containers.DeclarativeContainer):
    config = providers.Singleton(Config)

    db_engine: Engine = providers.Singleton(make_db_engine, config=config)

    session_factory: typing.Callable = providers.Singleton(
        make_session_factory, engine=db_engine
    )

    session: DBSession = providers.Singleton(make_session, factory=session_factory)

    dispatch_service: DispatchService = providers.Singleton(
        make_dispatch_service, session=session
    )

    worker_service: WorkerService = providers.Singleton(
        make_worker_service, session=session
    )
