import click

from .cli import cli
from .environment import Environment
from .environment import pass_env


@cli.command(name="process", help="Process BeanQueue tasks")
@click.argument("channels", nargs=-1)
@click.option(
    "-a", "--app", type=str, help='BeanQueue app object to use, e.g. "my_pkgs.bq.app"'
)
@pass_env
def main(
    env: Environment,
    channels: tuple[str, ...],
):
    env.app.process_tasks(channels)
