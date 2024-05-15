import logging

import click
from sqlalchemy.engine import create_engine
from sqlalchemy.pool import SingletonThreadPool

from .. import models
from ..db.session import Session
from ..processors.registry import collect
from ..services.dispatch import DispatchService


@click.command()
@click.argument("channels", nargs=-1)
@click.option(
    "-l",
    "--batch-size",
    type=int,
    default=1,
    help="Size of tasks batch to fetch each time from the database",
)
@click.option(
    "--pull-timeout",
    type=int,
    default=60,
    help="How long we should poll before timeout in seconds",
)
def main(
    channels: tuple[str],
    batch_size: int,
    pull_timeout: int,
):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # FIXME: the uri from opt
    engine = create_engine(
        "postgresql://bq:@localhost/bq_test", poolclass=SingletonThreadPool
    )
    Session.bind = engine

    logger.info("Processing tasks in channels = %s", channels)

    dispatch_service = DispatchService()
    # FIXME:
    pkgs = []
    registry = collect(pkgs)

    worker = models.Worker()

    dispatch_service.listen(channels)

    while True:
        try:
            dispatch_service.poll(timeout=pull_timeout)
        except TimeoutError:
            logger.debug("Poll timeout, try again")
            continue
        for task in dispatch_service.dispatch(
            channels, worker=worker, limit=batch_size
        ):
            logger.info(
                "Processing task %s, channel=%s, module=%s, func=%s",
                task.id,
                task.channel,
                task.module,
                task.func_name,
            )
            registry.process(task)


if __name__ == "__main__":
    main()
