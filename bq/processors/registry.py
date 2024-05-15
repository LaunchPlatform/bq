import collections
import dataclasses
import inspect
import logging
import typing

import venusian
from sqlalchemy.orm import object_session

from bq import models

BQ_PROCESSOR_CATEGORY = "bq_processor"


@dataclasses.dataclass(frozen=True)
class Processor:
    channel: str
    module: str
    name: str
    func: typing.Callable
    # should we auto complete the task or not
    auto_complete: bool = True
    # should we auto rollback the transaction when encounter unhandled exception
    auto_rollback_on_exc: bool = True


def process_task(task: models.Task, processor: Processor):
    logger = logging.getLogger(__name__)
    db = object_session(task)
    func_signature = inspect.signature(processor.func)
    base_kwargs = {}
    if "task" in func_signature.parameters:
        base_kwargs["task"] = task
    if "db" in func_signature.parameters:
        base_kwargs["db"] = db
    try:
        result = processor.func(**base_kwargs, **task.kwargs)
        if processor.auto_complete:
            logger.info("Task %s auto complete", task.id)
            task.state = models.TaskState.DONE
            db.add(task)
            db.commit()
        return result
    except Exception:
        logger.error("Unhandled exception for task %s", task.id, exc_info=True)
        if processor.auto_rollback_on_exc:
            db.rollback()
        # TODO: add error event
        task.state = models.TaskState.FAILED
        db.add(task)
        db.commit()


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
            db.add(task)
            db.commit()
            return
        return process_task(task, processor)


def processor(
    channel: str, auto_complete: bool = True, auto_rollback_on_exc: bool = True
) -> typing.Callable:
    def decorator(wrapped: typing.Callable):
        def callback(scanner: venusian.Scanner, name: str, ob: typing.Callable):
            processor = Processor(
                module=ob.__module__,
                name=name,
                channel=channel,
                func=ob,
                auto_complete=auto_complete,
                auto_rollback_on_exc=auto_rollback_on_exc,
            )
            scanner.registry.add(processor)

        venusian.attach(wrapped, callback, category=BQ_PROCESSOR_CATEGORY)
        return wrapped

    return decorator


def collect(packages: list[typing.Any], registry: Registry | None = None) -> Registry:
    if registry is None:
        registry = Registry()
    scanner = venusian.Scanner(registry=registry)
    for package in packages:
        scanner.scan(package, categories=(BQ_PROCESSOR_CATEGORY,))
    return registry
