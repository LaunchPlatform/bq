import typing

import pytest
from sqlalchemy.orm import Session

from bq import models
from bq.processors.processor import current_task
from bq.processors.processor import Processor


@pytest.mark.parametrize(
    "func, expected",
    [
        (lambda: [], []),
        (lambda task: ["task"], ["task"]),
        (lambda task, db: ["task", "db"], ["task", "db"]),
    ],
)
def test_process_task_kwargs(
    db: Session, task: models.Task, func: typing.Callable, expected: list
):
    processor = Processor(
        channel="mock-channel", module="mock.module", name="my_func", func=func
    )
    assert frozenset(processor.process(task=task)) == frozenset(expected)


@pytest.mark.parametrize("task__state", [models.TaskState.PROCESSING])
@pytest.mark.parametrize(
    "auto_complete, expected_state",
    [
        (True, models.TaskState.DONE),
        (False, models.TaskState.PROCESSING),
    ],
)
def test_process_task_auto_complete(
    db: Session,
    task: models.Task,
    auto_complete: bool,
    expected_state: models.TaskState,
):
    called = False

    def func():
        nonlocal called
        called = True
        return "result"

    processor = Processor(
        channel="mock-channel",
        module="mock.module",
        name="my_func",
        func=func,
        auto_complete=auto_complete,
    )
    assert processor.process(task=task) == "result"
    db.commit()
    assert task.state == expected_state
    assert called


def test_process_task_unhandled_exception(
    db: Session,
    task: models.Task,
):
    def func():
        raise ValueError("boom")

    processor = Processor(
        channel="mock-channel",
        module="mock.module",
        name="my_func",
        func=func,
    )
    processor.process(task=task)
    db.commit()
    assert task.state == models.TaskState.FAILED


@pytest.mark.parametrize("task__func_name", ["my_func"])
@pytest.mark.parametrize(
    "auto_rollback_on_exc, expected_func_name",
    [
        (True, "my_func"),
        (False, "changed"),
    ],
)
def test_process_task_auto_rollback_on_exc(
    db: Session, task: models.Task, auto_rollback_on_exc: bool, expected_func_name: str
):
    def func():
        task.func_name = "changed"
        db.add(task)
        db.flush()
        raise ValueError("boom")

    processor = Processor(
        channel="mock-channel",
        module="mock.module",
        name="my_func",
        func=func,
        auto_rollback_on_exc=auto_rollback_on_exc,
    )
    processor.process(task=task)
    db.commit()
    assert task.state == models.TaskState.FAILED
    assert task.func_name == expected_func_name


def test_processor_helper(processor_module: str):
    from ..fixtures.processors import processor0

    task = processor0.run(k0="v0")
    assert isinstance(task, models.Task)
    assert task.module == processor_module
    assert task.func_name == "processor0"
    assert task.channel == "mock-channel"
    assert task.kwargs == dict(k0="v0")
    assert task.parent is None
    assert not task.children


def test_processor_helper_create_child_task(
    db: Session, processor_module: str, task: models.Task
):
    from ..fixtures.processors import processor0

    token = current_task.set(task)
    try:
        child_task = processor0.run(k0="v0")
        db.add(child_task)
        db.commit()
    finally:
        current_task.reset(token)

    db.expire_all()
    assert child_task.parent == task
    assert task.children == [child_task]
