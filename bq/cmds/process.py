import functools
import importlib
import logging
import platform
import sys
import threading
import time
import typing

import click
from dependency_injector.wiring import inject
from dependency_injector.wiring import Provide
from sqlalchemy import func
from sqlalchemy.orm import Session as DBSession

from .. import constants
from .. import models
from ..config import Config
from ..container import Container
from ..processors.registry import collect
from ..services.dispatch import DispatchService
from ..services.worker import WorkerService


@click.command()
@click.argument("channels", nargs=-1)
def main(
    channels: tuple[str, ...],
):
    process_tasks(channels)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    container = Container()
    container.wire(modules=[__name__])
    main()
