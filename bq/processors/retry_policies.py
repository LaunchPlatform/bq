import datetime
import typing

from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy.orm import object_session

from .. import models


class Delay:
    def __init__(self, delay: datetime.timedelta):
        self.delay = delay

    def __call__(self, task: models.Task) -> typing.Any:
        return func.now() + self.delay


class ExponentialBackoff:
    def __init__(
        self, base: float = 2, exponent_offset: float = 0, exponent_scalar: float = 1.0
    ):
        self.base = base
        self.exponent_offset = exponent_offset
        self.exponent_scalar = exponent_scalar

    def __call__(self, task: models.Task) -> typing.Any:
        db = object_session(task)
        task_info = inspect(task.__class__)
        event_cls = task_info.attrs["events"].entity.class_
        failed_times = (
            db.query(event_cls)
            .filter(event_cls.task == task)
            .filter(event_cls.type == models.EventType.FAILED_RETRY_SCHEDULED)
        ).count()
        delay_seconds = self.base ** (
            self.exponent_offset + (self.exponent_scalar * (failed_times + 1))
        )
        return func.now() + datetime.timedelta(seconds=delay_seconds)
