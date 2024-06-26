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


def test_process_task_events(
    db: Session,
    task: models.Task,
):
    def func():
        return "result"

    processor = Processor(
        channel="mock-channel",
        module="mock.module",
        name="my_func",
        func=func,
        auto_complete=True,
    )
    assert processor.process(task=task, event_cls=models.Event) == "result"
    db.commit()
    db.expire_all()
    assert len(task.events) == 1
    event = task.events[0]
    assert event.type == models.EventType.COMPLETE
    assert event.error_message is None
    assert event.scheduled_at is None


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
def test_process_savepoint_rollback(
    db: Session,
    task: models.Task,
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
    )
    processor.process(task=task)
    db.commit()
    assert task.state == models.TaskState.FAILED
    assert task.func_name == "my_func"


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
