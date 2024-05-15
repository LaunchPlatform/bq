import collections
import logging
import typing

import venusian
from sqlalchemy.orm import object_session

from bq import models

BQ_PROCESSOR_CATEGORY = "bq_processor"


class Processor(typing.NamedTuple):
    module: str
    name: str
    channel: str
    func: typing.Callable


class Registry:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.processors = collections.defaultdict(lambda: collections.defaultdict(dict))

    def add(self, processor: Processor):
        self.processors[processor.channel][processor.module][processor.name] = processor

    def process(self, task: models.Task) -> typing.Any:
        modules = self.processors.get(task.channel, {})
        functions = modules.get(task.module, {})
        func = functions.get(task.func_name)
        db = object_session(task)
        if func is None:
            self.logger.error(
                "Cannot find processor for task %s with func=%s, module=%s",
                task.id,
                task.func_name,
                task.module,
            )
            # TODO: add error event
            task.state = models.TaskState.FAILED
            db.add(task)
            db.commit()
            return
        try:
            return func(db=db, task=task, **task.kwargs)
        except Exception:
            self.logger.error("Unprocessed error for task %s", task.id, exc_info=True)
            # TODO: add error event
            task.state = models.TaskState.FAILED
            db.add(task)
            db.commit()


def processor(channel: str) -> typing.Callable:
    def decorator(wrapped: typing.Callable):
        def callback(scanner: venusian.Scanner, name: str, ob: typing.Callable):
            processor = Processor(
                module=ob.__module__, name=name, channel=channel, func=ob
            )
            scanner.registry.add(processor)

        venusian.attach(wrapped, callback, category=BQ_PROCESSOR_CATEGORY)
        return wrapped

    return decorator


def collect(
    packages: list[typing.Any],
) -> dict[str, dict]:
    registry = Registry()
    scanner = venusian.Scanner(registry=registry)
    for package in packages:
        scanner.scan(package, categories=(BQ_PROCESSOR_CATEGORY,))
    return registry.processors
