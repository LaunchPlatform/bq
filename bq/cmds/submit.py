import json

import click

from .cli import cli
from .environment import Environment
from .environment import pass_env


@cli.command(name="submit", help="Submit a new task, mostly for debugging purpose")
@click.argument("channel", nargs=1)
@click.argument("module", nargs=1)
@click.argument("func", nargs=1)
@click.option(
    "-k", "--kwargs", type=str, help="Keyword arguments as JSON", default=None
)
@pass_env
def submit(
    env: Environment,
    channel: str,
    module: str,
    func: str,
    kwargs: str | None,
):
    db = env.app.make_session()

    env.logger.info(
        "Submit task with channel=%s, module=%s, func=%s", channel, module, func
    )
    kwargs_value = {}
    if kwargs:
        kwargs_value = json.loads(kwargs)

    task = env.app.task_model(
        channel=channel,
        module=module,
        func_name=func,
        kwargs=kwargs_value,
    )
    db.add(task)
    db.commit()
    env.logger.info("Done, submit task %s", task.id)
