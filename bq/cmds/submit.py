import json
import logging

import click
from dependency_injector.wiring import inject
from dependency_injector.wiring import Provide

from .. import models
from ..container import Container
from ..db.session import Session


@click.command()
@click.argument("channel", nargs=1)
@click.argument("module", nargs=1)
@click.argument("func", nargs=1)
@click.option(
    "-k", "--kwargs", type=str, help="Keyword arguments as JSON", default=None
)
@inject
def main(
    channel: str,
    module: str,
    func: str,
    kwargs: str | None,
    db: Session = Provide[Container.session],
):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info(
        "Submit task with channel=%s, module=%s, func=%s", channel, module, func
    )
    kwargs_value = {}
    if kwargs:
        kwargs_value = json.loads(kwargs)
    task = models.Task(
        channel=channel, module=module, func_name=func, kwargs=kwargs_value
    )
    db.add(task)
    db.commit()
    logger.info("Done, submit task %s", task.id)


if __name__ == "__main__":
    container = Container()
    container.wire(modules=[__name__])
    main()
