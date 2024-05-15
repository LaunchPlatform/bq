import importlib
import logging
import platform

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
    "-p",
    "--packages",
    type=str,
    help="Packages to scan for processor functions",
    required=True,
    multiple=True,
)
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
    channels: tuple[str, ...],
    packages: tuple[str, ...],
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

    logger.info("Scanning packages %s", packages)
    pkgs = list(map(importlib.import_module, packages))
    registry = collect(pkgs)
    for channel, module_processors in registry.processors.items():
        logger.info("Collected processors with channel %r", channel)
        for module, func_processors in module_processors.items():
            for processor in func_processors.values():
                logger.info(
                    "  Processor module %r, processor %r", module, processor.name
                )

    dispatch_service = DispatchService()
    db = Session()
    worker = models.Worker(name=platform.node())
    db.add(worker)
    dispatch_service.listen(channels)
    db.flush()
    logger.info("Created worker %s, name=%s", worker.id, worker.name)
    db.commit()

    while True:
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
        try:
            for notification in dispatch_service.poll(timeout=pull_timeout):
                logger.debug("Receive notification %s", notification)
        except TimeoutError:
            logger.debug("Poll timeout, try again")
            continue


if __name__ == "__main__":
    main()
