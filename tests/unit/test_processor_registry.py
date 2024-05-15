import typing

import pytest
from sqlalchemy.orm import Session

from . import fixtures
from bq import models
from bq.processors.registry import collect
from bq.processors.registry import process_task
from bq.processors.registry import Processor
from bq.processors.registry import Registry


@pytest.fixture
def registry() -> Registry:
    return collect([fixtures])


@pytest.fixture
def processor_module() -> str:
    return ".".join(__name__.split(".")[:-1]) + ".fixtures.processors"


def test_collect(registry: Registry, processor_module: str):
    assert registry.processors.keys() == {"mock-channel", "mock-channel2"}

    modules0 = registry.processors["mock-channel"]
    assert modules0.keys() == {processor_module}
    funcs0 = modules0[processor_module]
    assert funcs0.keys() == {"processor0"}

    modules1 = registry.processors["mock-channel2"]
    assert modules1.keys() == {processor_module}
    funcs1 = modules1[processor_module]
    assert funcs1.keys() == {"processor1"}


@pytest.mark.parametrize(
    "task__channel, task__module, task__func_name, task__kwargs, expected",
    [
        (
            "mock-channel",
            "tests.unit.fixtures.processors",
            "processor0",
            {},
            "processed by processor0",
        ),
        (
            "mock-channel2",
            "tests.unit.fixtures.processors",
            "processor1",
            dict(kwarg0="mock-val"),
            "mock-val",
        ),
    ],
)
def test_registry_process(
    db: Session, registry: Registry, task: models.Task, expected: str
):
    assert registry.process(task) == expected


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
    assert process_task(task=task, processor=processor) == "result"
    db.expire_all()
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
    process_task(task=task, processor=processor)
    db.expire_all()
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
    process_task(task=task, processor=processor)
    db.expire_all()
    assert task.state == models.TaskState.FAILED
    assert task.func_name == expected_func_name
