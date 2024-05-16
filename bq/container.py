from dependency_injector import containers
from dependency_injector import providers
from sqlalchemy import create_engine
from sqlalchemy import Engine
from sqlalchemy.pool import SingletonThreadPool

from .config import Config


def build_engine(config: Config) -> Engine:
    return create_engine(str(config.DATABASE_URL), poolclass=SingletonThreadPool)


class Container(containers.DeclarativeContainer):
    config = providers.Singleton(Config)

    db_engine: Engine = providers.Singleton(build_engine, config=config)
