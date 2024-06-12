import contextvars
import dataclasses
import datetime
import inspect
import logging
import typing

from sqlalchemy import select
from sqlalchemy.orm import object_session

from .. import events
from .. import models

logger = logging.getLogger(__name__)
current_task = contextvars.ContextVar("current_task")


@dataclasses.dataclass(frozen=True)
class Processor:
    channel: str
    module: str
    name: str
    func: typing.Callable
    # should we auto complete the task or not
    auto_complete: bool = True
    # The retry policy function for returning a new scheduled time for next attempt
    retry_policy: typing.Callable | None = None
    # The exceptions we suppose to retry when encountered
    retry_exceptions: typing.Type | typing.Tuple[typing.Type, ...] | None = None

    def process(self, task: models.Task, event_cls: typing.Type | None = None):
        ctx_token = current_task.set(task)
        try:
            db = object_session(task)
            func_signature = inspect.signature(self.func)
            base_kwargs = {}
            if "task" in func_signature.parameters:
                base_kwargs["task"] = task
            if "db" in func_signature.parameters:
                base_kwargs["db"] = db
            try:
                with db.begin_nested() as savepoint:
                    if "savepoint" in func_signature.parameters:
                        base_kwargs["savepoint"] = savepoint
                    result = self.func(**base_kwargs, **task.kwargs)
            except Exception as exc:
                logger.error("Unhandled exception for task %s", task.id, exc_info=True)
                events.task_failure.send(self, task=task, exception=exc)
                task.state = models.TaskState.FAILED
                task.error_message = str(exc)
                retry_scheduled_at = None
                if (
                    self.retry_exceptions is None
                    or isinstance(exc, self.retry_exceptions)
                ) and self.retry_policy is not None:
                    retry_scheduled_at = self.retry_policy(task)
                    if retry_scheduled_at is not None:
                        task.state = models.TaskState.PENDING
                        task.scheduled_at = retry_scheduled_at
                        if isinstance(retry_scheduled_at, datetime.datetime):
                            retry_scheduled_at_value = retry_scheduled_at
                        else:
                            retry_scheduled_at_value = db.scalar(
                                select(retry_scheduled_at)
                            )
                        logger.info(
                            "Schedule task %s for retry at %s",
                            task.id,
                            retry_scheduled_at_value,
                        )
                if event_cls is not None:
                    event = event_cls(
                        task=task,
                        type=models.EventType.FAILED
                        if retry_scheduled_at is None
                        else models.EventType.FAILED_RETRY_SCHEDULED,
                        error_message=task.error_message,
                        scheduled_at=retry_scheduled_at,
                    )
                    db.add(event)
                db.add(task)
                return
            if self.auto_complete:
                logger.info("Task %s auto complete", task.id)
                task.state = models.TaskState.DONE
                task.result = result
                if event_cls is not None:
                    event = event_cls(
                        task=task,
                        type=models.EventType.COMPLETE,
                    )
                    db.add(event)
                db.add(task)
            return result
        finally:
            current_task.reset(ctx_token)


class ProcessorHelper:
    """Helper function to replace the decorated processor function and make creating Task model much easier"""

    def __init__(self, processor: Processor, task_cls: typing.Type = models.Task):
        self._processor = processor
        self._task_cls = task_cls

    def __call__(self, *args, **kwargs):
        return self._processor.func(*args, **kwargs)

    def run(self, **kwargs) -> models.Task:
        try:
            parent = current_task.get()
        except LookupError:
            parent = None
        return self._task_cls(
            channel=self._processor.channel,
            module=self._processor.module,
            func_name=self._processor.name,
            kwargs=kwargs,
            parent=parent,
        )
