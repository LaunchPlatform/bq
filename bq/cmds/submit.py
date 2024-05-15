import logging

import click
from sqlalchemy.engine import create_engine
from sqlalchemy.pool import SingletonThreadPool

from .. import models
from ..db.session import Session
from ..services.dispatch import DispatchService


@click.command()
@click.argument("channel", nargs=1)
@click.option(
    "-m",
    "--module",
    type=str,
    help='Name of module, such as "mymodule.tasks.payments"',
)
@click.option(
    "-f",
    "--func",
    type=str,
    help='Name of function, such as "process_payment"',
)
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
    Session.bind = engine

    dispatch_service = DispatchService()

    db = Session()
    logger.info(
        "Submit task with channel=%s, module=%s, func=%s", channel, module, func
    )
    task = models.Task(channel=channel, module=module, func_name=func, kwargs={})
    db.add(task)
    dispatch_service.notify([channel])
    db.commit()
    logger.info("Done")


if __name__ == "__main__":
    main()
