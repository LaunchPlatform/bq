import datetime

import pytest
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from ...factories import TaskFactory
from bq import models
from bq.services.dispatch import DispatchService


@pytest.fixture
async def dispatch_service(async_db: AsyncSession) -> DispatchService:
    service = DispatchService(async_db)
    try:
        yield service
    finally:
        await service.aclose()


async def test_dispatch_empty(
    async_db: AsyncSession, dispatch_service: DispatchService, worker: models.Worker
):
    assert not await dispatch_service.dispatch(["test"], worker_id=worker.id)


async def test_dispatch(
    db: Session,
    async_db: AsyncSession,
    dispatch_service: DispatchService,
    worker: models.Worker,
    task: models.Task,
):
    assert task.state == models.TaskState.PENDING
    tasks = await dispatch_service.dispatch([task.channel], worker_id=worker.id)
    await async_db.commit()
    db.expire_all()
    assert len(tasks) == 1
    returned_task = tasks[0]
    assert returned_task.state == models.TaskState.PROCESSING
    assert returned_task.worker_id == worker.id
    assert not await dispatch_service.dispatch([task.channel], worker_id=worker.id)


@pytest.mark.parametrize(
    "task__scheduled_at", [func.now() + datetime.timedelta(seconds=10)]
)
async def test_dispatch_with_scheduled_at(
    async_db: AsyncSession,
    dispatch_service: DispatchService,
    worker: models.Worker,
    task: models.Task,
):
    assert task.state == models.TaskState.PENDING
    assert task.scheduled_at is not None

    tasks = await dispatch_service.dispatch([task.channel], worker_id=worker.id)
    await async_db.commit()
    assert len(tasks) == 0

    tasks = await dispatch_service.dispatch(
        [task.channel],
        worker_id=worker.id,
        now=func.now() + datetime.timedelta(seconds=10),
    )
    await async_db.commit()
    assert len(tasks) == 1
    returned_task = tasks[0]
    assert returned_task.state == models.TaskState.PROCESSING
    assert returned_task.worker_id == worker.id


async def test_dispatch_many(
    db: Session,
    async_db: AsyncSession,
    dispatch_service: DispatchService,
    worker: models.Worker,
    task_factory: TaskFactory,
):
    for _ in range(3):
        task_factory(channel="other_channel")

    channel = "my_channel"
    for _ in range(4):
        task_factory(channel=channel)

    task_factory(channel=channel, state=models.TaskState.DONE)

    tasks = await dispatch_service.dispatch([channel], worker_id=worker.id, limit=3)
    await async_db.commit()
    db.expire_all()
    assert len(tasks) == 3
    for task in tasks:
        assert task.state == models.TaskState.PROCESSING
        assert task.worker_id == worker.id

    for task in db.query(models.Task).filter(models.Task.channel != channel):
        assert task.state == models.TaskState.PENDING
        assert task.worker is None

    remain_ids = list(
        db.query(models.Task.id)
        .filter(models.Task.channel == channel)
        .filter(models.Task.state == models.TaskState.PENDING)
    )
    assert len(remain_ids) == 1
    assert remain_ids[0] not in [task.id for task in tasks]

    tasks = await dispatch_service.dispatch(["my_channel"], worker_id=worker.id)
    assert len(tasks) == 1


async def test_listen_value_quote(
    async_db: AsyncSession, dispatch_service: DispatchService
):
    await dispatch_service.listen(["a", "中文", "!@#$%^&*(()-_"])
    await async_db.commit()


async def test_poll(async_db: AsyncSession, dispatch_service: DispatchService):
    await dispatch_service.listen(["a", "b", "c"])
    await async_db.commit()
    with pytest.raises(TimeoutError):
        await dispatch_service.poll(timeout=1)
    await dispatch_service.notify(["a", "c"])
    await async_db.commit()
    notifications = await dispatch_service.poll(timeout=1)
    assert frozenset([n.channel for n in notifications]) == frozenset(["a", "c"])
