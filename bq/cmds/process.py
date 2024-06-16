import click

from .cli import cli
from .environment import Environment
from .environment import pass_env


@cli.command(name="process", help="Process BeanQueue tasks")
@click.argument("channels", nargs=-1)
@pass_env
def process(
    env: Environment,
    channels: tuple[str, ...],
):
    env.app.process_tasks(channels)
