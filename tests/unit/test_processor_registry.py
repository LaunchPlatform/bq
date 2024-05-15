import typing

import pytest
from sqlalchemy.orm import Session

from bq import models
from bq.processors.registry import collect
from bq.processors.registry import process_task
from bq.processors.registry import Processor


def test_collect():
    from . import fixtures

    registry = collect([fixtures])
    assert registry.keys() == {"mock-channel", "mock-channel2"}
    module_name = ".".join(__name__.split(".")[:-1]) + ".fixtures.processors"

    modules0 = registry["mock-channel"]
    assert modules0.keys() == {module_name}
    funcs0 = modules0[module_name]
    assert funcs0.keys() == {"processor0"}

    modules1 = registry["mock-channel2"]
    assert modules1.keys() == {module_name}
    funcs1 = modules1[module_name]
    assert funcs1.keys() == {"processor1"}


@pytest.mark.parametrize(
    "func, expected",
    [
        (lambda: [], []),
        (lambda task: {"task"}, ["task"]),
        (lambda task, db: {"task", "db"}, ["task", "db"]),
    ],
)
def test_process_task_kwargs(
    db: Session, task: models.Task, func: typing.Callable, expected: list
):
    processor = Processor(
        channel="mock-channel", module="mock.module", name="my_func", func=func
    )
    assert frozenset(process_task(task=task, processor=processor)) == frozenset(
        expected
    )
