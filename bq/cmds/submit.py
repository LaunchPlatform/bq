import logging

import click
from sqlalchemy.engine import create_engine
from sqlalchemy.pool import SingletonThreadPool

from .. import models
from ..db.session import Session


@click.command()
@click.argument("channel", nargs=1)
@click.argument("module", nargs=1)
@click.argument("func", nargs=1)
def main(
    channel: str,
    module: str,
    func: str,
):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # FIXME: the uri from opt
    engine = create_engine(
        "postgresql://bq:@localhost/bq_test", poolclass=SingletonThreadPool
    )
    Session.configure(bind=engine)
    db = Session()

    logger.info(
        "Submit task with channel=%s, module=%s, func=%s", channel, module, func
    )
    task = models.Task(channel=channel, module=module, func_name=func, kwargs={})
    db.add(task)
    db.commit()
    logger.info("Done, submit task %s", task.id)


if __name__ == "__main__":
    main()
