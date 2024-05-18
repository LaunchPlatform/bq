import json
import logging

import click

from .. import models
from .utils import load_app

logger = logging.getLogger(__name__)


@click.command()
@click.argument("channel", nargs=1)
@click.argument("module", nargs=1)
@click.argument("func", nargs=1)
@click.option(
    "-k", "--kwargs", type=str, help="Keyword arguments as JSON", default=None
)
@click.option(
    "-a", "--app", type=str, help='BeanQueue app object to use, e.g. "my_pkgs.bq.app"'
)
def main(
    channel: str,
    module: str,
    func: str,
    kwargs: str | None,
    app: str | None = None,
):
    app = load_app(app)
    db = app.session_cls(bind=app.create_default_engine())

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
    logging.basicConfig(level=logging.INFO)
    main()
