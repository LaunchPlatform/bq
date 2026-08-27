import asyncio
import contextvars
import dataclasses
import datetime
import inspect
import logging
import typing

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_object_session
from sqlalchemy.ext.asyncio import AsyncSession
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

    async def process(self, task: models.Task, event_cls: typing.Type | None = None):
        ctx_token = current_task.set(task)
        try:
            db = async_object_session(task)
            if db is None:
                db = object_session(task)
            if db is None:
                raise RuntimeError("Task is not attached to a database session")
            func_signature = inspect.signature(self.func)
            base_kwargs: dict[str, typing.Any] = {}
            if "task" in func_signature.parameters:
                base_kwargs["task"] = task
            try:
                result = await self._invoke(db, task, func_signature, base_kwargs)
            except Exception as exc:
                if isinstance(db, AsyncSession):
                    await db.refresh(task)
                logger.error("Unhandled exception for task %s", task.id, exc_info=True)
                events.task_failure.send(self, task=task, exception=exc)
                task.state = models.TaskState.FAILED
                task.error_message = str(exc)
                retry_scheduled_at = None
                if (
                    self.retry_exceptions is None
                    or isinstance(exc, self.retry_exceptions)
                ) and self.retry_policy is not None:
                    retry_scheduled_at = await self._invoke_retry_policy(db, task)
                    if retry_scheduled_at is not None:
                        task.state = models.TaskState.PENDING
                        task.scheduled_at = retry_scheduled_at
                        if isinstance(retry_scheduled_at, datetime.datetime):
                            retry_scheduled_at_value = retry_scheduled_at
                        else:
                            retry_scheduled_at_value = await _session_scalar(
                                db, retry_scheduled_at
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

    async def _invoke(
        self,
        db: typing.Any,
        task: models.Task,
        func_signature: inspect.Signature,
        base_kwargs: dict[str, typing.Any],
    ) -> typing.Any:
        kwargs = dict(task.kwargs or {})
        wants_db = "db" in func_signature.parameters
        wants_savepoint = "savepoint" in func_signature.parameters
        is_async = inspect.iscoroutinefunction(self.func)

        if is_async:
            async with db.begin_nested() as savepoint:
                call_kwargs = dict(base_kwargs)
                if wants_db:
                    call_kwargs["db"] = db
                if wants_savepoint:
                    call_kwargs["savepoint"] = savepoint
                return await self.func(**call_kwargs, **kwargs)

        if wants_db:
            if isinstance(db, AsyncSession):

                def _sync_call(sync_db: typing.Any) -> typing.Any:
                    call_kwargs = dict(base_kwargs)
                    call_kwargs["db"] = sync_db
                    with sync_db.begin_nested() as savepoint:
                        if wants_savepoint:
                            call_kwargs["savepoint"] = savepoint
                        return self.func(**call_kwargs, **kwargs)

                return await db.run_sync(_sync_call)

            with db.begin_nested() as savepoint:
                call_kwargs = dict(base_kwargs)
                call_kwargs["db"] = db
                if wants_savepoint:
                    call_kwargs["savepoint"] = savepoint
                return self.func(**call_kwargs, **kwargs)

        if isinstance(db, AsyncSession):
            async with db.begin_nested() as savepoint:
                call_kwargs = dict(base_kwargs)
                if wants_savepoint:
                    call_kwargs["savepoint"] = savepoint
                return await asyncio.to_thread(self.func, **call_kwargs, **kwargs)

        with db.begin_nested() as savepoint:
            call_kwargs = dict(base_kwargs)
            if wants_savepoint:
                call_kwargs["savepoint"] = savepoint
            return self.func(**call_kwargs, **kwargs)

    async def _invoke_retry_policy(self, db: typing.Any, task: models.Task) -> typing.Any:
        if inspect.iscoroutinefunction(self.retry_policy):
            return await self.retry_policy(task)
        if isinstance(db, AsyncSession):
            return await db.run_sync(lambda _: self.retry_policy(task))
        return self.retry_policy(task)


async def _session_scalar(db: typing.Any, expression: typing.Any) -> typing.Any:
    if isinstance(db, AsyncSession):
        return await db.scalar(select(expression))
    return db.scalar(select(expression))


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
