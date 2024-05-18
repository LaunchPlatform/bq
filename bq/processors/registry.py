import collections
import logging
import typing

import venusian
from sqlalchemy.orm import object_session

from .. import constants
from .. import models
from .processor import Processor


class Registry:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.processors = collections.defaultdict(lambda: collections.defaultdict(dict))

    def add(self, processor: Processor):
        self.processors[processor.channel][processor.module][processor.name] = processor

    def process(self, task: models.Task) -> typing.Any:
        modules = self.processors.get(task.channel, {})
        functions = modules.get(task.module, {})
        processor = functions.get(task.func_name)
        db = object_session(task)
        if processor is None:
            self.logger.error(
                "Cannot find processor for task %s with module=%s, func=%s",
                task.id,
                task.module,
                task.func_name,
            )
            # TODO: add error event
            task.state = models.TaskState.FAILED
            task.error_message = f"Cannot find processor for task with module={task.module}, func={task.func_name}"
            db.add(task)
            return
        return processor.process(task)


def collect(packages: list[typing.Any], registry: Registry | None = None) -> Registry:
    if registry is None:
        registry = Registry()
    scanner = venusian.Scanner(registry=registry)
    for package in packages:
        scanner.scan(package, categories=(constants.BQ_PROCESSOR_CATEGORY,))
    return registry
