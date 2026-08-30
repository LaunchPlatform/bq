import typing

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
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
async def test_process_task_kwargs(
    async_db: AsyncSession, task: models.Task, func: typing.Callable, expected: list
):
    task = await async_db.get(models.Task, task.id)
    processor = Processor(
        channel="mock-channel", module="mock.module", name="my_func", func=func
    )
    assert frozenset(await processor.process(task=task)) == frozenset(expected)


@pytest.mark.parametrize("task__state", [models.TaskState.PROCESSING])
@pytest.mark.parametrize(
    "auto_complete, expected_state",
    [
        (True, models.TaskState.DONE),
        (False, models.TaskState.PROCESSING),
    ],
)
async def test_process_task_auto_complete(
    async_db: AsyncSession,
    task: models.Task,
    auto_complete: bool,
    expected_state: models.TaskState,
):
    called = False

    def func():
        nonlocal called
        called = True
        return "result"

    task = await async_db.get(models.Task, task.id)
    processor = Processor(
        channel="mock-channel",
        module="mock.module",
        name="my_func",
        func=func,
        auto_complete=auto_complete,
    )
    assert await processor.process(task=task) == "result"
    await async_db.commit()
    assert task.state == expected_state
    assert called


async def test_process_task_events(
    async_db: AsyncSession,
    task: models.Task,
):
    def func():
        return "result"

    task = await async_db.get(models.Task, task.id)
    processor = Processor(
        channel="mock-channel",
        module="mock.module",
        name="my_func",
        func=func,
        auto_complete=True,
    )
    assert await processor.process(task=task, event_cls=models.Event) == "result"
    await async_db.commit()
    await async_db.refresh(task, attribute_names=["events"])
    assert len(task.events) == 1
    event = task.events[0]
    assert event.type == models.EventType.COMPLETE
    assert event.error_message is None
    assert event.scheduled_at is None


async def test_process_task_unhandled_exception(
    async_db: AsyncSession,
    task: models.Task,
):
    def func():
        raise ValueError("boom")

    task = await async_db.get(models.Task, task.id)
    processor = Processor(
        channel="mock-channel",
        module="mock.module",
        name="my_func",
        func=func,
    )
    await processor.process(task=task)
    await async_db.commit()
    assert task.state == models.TaskState.FAILED


@pytest.mark.parametrize("task__func_name", ["my_func"])
async def test_process_savepoint_rollback(
    async_db: AsyncSession,
    task: models.Task,
):
    def func(db, task):
        task.func_name = "changed"
        db.add(task)
        db.flush()
        raise ValueError("boom")

    task = await async_db.get(models.Task, task.id)
    processor = Processor(
        channel="mock-channel",
        module="mock.module",
        name="my_func",
        func=func,
    )
    await processor.process(task=task)
    await async_db.commit()
    await async_db.refresh(task)
    assert task.state == models.TaskState.FAILED
    assert task.func_name == "my_func"


async def test_process_async_processor(
    async_db: AsyncSession,
    task: models.Task,
):
    async def func(task, db):
        task.error_message = "from-async"
        db.add(task)
        return "async-result"

    task = await async_db.get(models.Task, task.id)
    processor = Processor(
        channel="mock-channel",
        module="mock.module",
        name="my_func",
        func=func,
    )
    assert await processor.process(task=task) == "async-result"
    await async_db.commit()
    assert task.state == models.TaskState.DONE
    assert task.result == "async-result"


async def test_process_requires_async_session(db: Session, task: models.Task):
    processor = Processor(
        channel="mock-channel", module="mock.module", name="my_func", func=lambda: None
    )
    with pytest.raises(RuntimeError, match="AsyncSession"):
        await processor.process(task=task)


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


async def test_processor_helper_create_child_task(
    db: Session, async_db: AsyncSession, processor_module: str, task: models.Task
):
    from ..fixtures.processors import processor0

    task = await async_db.get(models.Task, task.id)
    token = current_task.set(task)
    try:
        child_task = processor0.run(k0="v0")
        async_db.add(child_task)
        await async_db.commit()
    finally:
        current_task.reset(token)

    await async_db.refresh(task, attribute_names=["children"])
    assert child_task.parent_id == task.id
    assert [child.id for child in task.children] == [child_task.id]
