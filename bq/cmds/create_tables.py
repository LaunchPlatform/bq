import logging

import click
from dependency_injector.wiring import inject
from dependency_injector.wiring import Provide
from sqlalchemy.engine import Engine

from .. import models  # noqa
from ..container import Container
from ..db.base import Base


@click.command()
@inject
def main(engine: Engine = Provide[Container.db_engine]):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    Base.metadata.create_all(bind=engine)
    logger.info("Done, tables created")


if __name__ == "__main__":
    container = Container()
    container.wire(modules=[__name__])
    main()
