import logging

import click

from . import models
from .processors.registry import collect
from .services.dispatch import DispatchService


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
    channels: list[str],
    batch_size: int,
    pull_timeout: int,
):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

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
            registry.process(task)


if __name__ == "__main__":
    main()
