import dataclasses
import inspect
import logging
import typing

from sqlalchemy.orm import object_session

from .. import models

logger = logging.getLogger(__name__)


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

    def process(self, task: models.Task):
        db = object_session(task)
        func_signature = inspect.signature(self.func)
        base_kwargs = {}
        if "task" in func_signature.parameters:
            base_kwargs["task"] = task
        if "db" in func_signature.parameters:
            base_kwargs["db"] = db
        with db.begin_nested() as savepoint:
            if "savepoint" in func_signature.parameters:
                base_kwargs["savepoint"] = savepoint
            try:
                result = self.func(**base_kwargs, **task.kwargs)
            except Exception as exc:
                logger.error("Unhandled exception for task %s", task.id, exc_info=True)
                if self.auto_rollback_on_exc:
                    savepoint.rollback()
                # TODO: add error event
                task.state = models.TaskState.FAILED
                task.error_message = str(exc)
                db.add(task)
                return
        if self.auto_complete:
            logger.info("Task %s auto complete", task.id)
            task.state = models.TaskState.DONE
            task.result = result
            db.add(task)
        return result


class ProcessorHelper:
    """Helper function to replace the decorated processor function and make creating Task model much easier"""

    def __init__(self, processor: Processor, task_cls: typing.Type = models.Task):
        self._processor = processor
        self._task_cls = task_cls

    def __call__(self, *args, **kwargs):
        return self._processor.func(*args, **kwargs)

    def run(self, **kwargs) -> models.Task:
        return self._task_cls(
            channel=self._processor.channel,
            module=self._processor.module,
            func_name=self._processor.name,
            kwargs=kwargs,
        )
