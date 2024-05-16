import logging

import click
from sqlalchemy.engine import create_engine
from sqlalchemy.pool import SingletonThreadPool

from ..db.base import Base


@click.command()
def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # FIXME: the uri from opt
    engine = create_engine(
        "postgresql://bq:@localhost/bq_test", poolclass=SingletonThreadPool
    )

    Base.metadata.create_all(bind=engine)
    logger.info("Done, tables created")


if __name__ == "__main__":
    main()
