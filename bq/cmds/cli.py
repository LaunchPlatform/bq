import logging
import os

import click
from rich.logging import RichHandler

from .environment import Environment
from .environment import LOG_LEVEL_MAP
from .environment import LogLevel
from .environment import pass_env
from .utils import load_app


@click.group(help="Command line tools for BeanQueue")
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(
        list(map(lambda key: key.value, LOG_LEVEL_MAP.keys())), case_sensitive=False
    ),
    default=lambda: os.environ.get("LOG_LEVEL", "INFO"),
)
@click.option(
    "--disable-rich-log",
    is_flag=True,
    help="disable rich log handler",
)
@click.option(
    "-a", "--app", type=str, help='BeanQueue app object to use, e.g. "my_pkgs.bq.app"'
)
@click.version_option(prog_name="bq", package_name="bq")
@pass_env
def cli(env: Environment, log_level: str, disable_rich_log: bool, app: str):
    env.log_level = LogLevel(log_level)
    env.app = load_app(app)

    if disable_rich_log:
        logging.basicConfig(
            level=LOG_LEVEL_MAP[env.log_level],
            force=True,
        )
    else:
        FORMAT = "%(message)s"
        logging.basicConfig(
            level=LOG_LEVEL_MAP[env.log_level],
            format=FORMAT,
            datefmt="[%X]",
            handlers=[RichHandler()],
            force=True,
        )
