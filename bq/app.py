import typing

import venusian

from . import constants
from .config import Config
from .processors.processor import Processor
from .processors.processor import ProcessorHelper
from .utils import get_model_class


class BeanQueue:
    def __init__(self, config: Config):
        self.config = config
        self.task_cls = get_model_class(config.TASK_MODEL)

    def processor(
        self,
        channel: str = constants.DEFAULT_CHANNEL,
        auto_complete: bool = True,
        auto_rollback_on_exc: bool = True,
        task_cls: typing.Type | None = None,
    ) -> typing.Callable:
        def decorator(wrapped: typing.Callable):
            processor = Processor(
                module=wrapped.__module__,
                name=wrapped.__name__,
                channel=channel,
                func=wrapped,
                auto_complete=auto_complete,
                auto_rollback_on_exc=auto_rollback_on_exc,
            )
            helper_obj = ProcessorHelper(
                processor, task_cls=task_cls if task_cls is not None else self.task_cls
            )

            def callback(scanner: venusian.Scanner, name: str, ob: typing.Callable):
                if processor.name != name:
                    raise ValueError("Name is not the same")
                scanner.registry.add(processor)

            venusian.attach(
                helper_obj, callback, category=constants.BQ_PROCESSOR_CATEGORY
            )
            return helper_obj

        return decorator
