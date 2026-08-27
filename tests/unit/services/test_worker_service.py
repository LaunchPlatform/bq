import datetime

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from ...factories import TaskFactory
from ...factories import WorkerFactory
from bq import models
from bq.services.worker import WorkerService


@pytest.fixture
def worker_service(async_db: AsyncSession) -> WorkerService:
    return WorkerService(async_db)


async def test_update_heartbeat(
    async_db: AsyncSession, worker_service: WorkerService, worker: models.Worker
):
    now = await async_db.scalar(select(func.now()))
    worker_row = await async_db.get(models.Worker, worker.id)
    assert worker_row.last_heartbeat != now
    worker_service.update_heartbeat(worker_row)
    await async_db.commit()
    await async_db.refresh(worker_row)
    assert worker_row.last_heartbeat == now


async def test_fetch_dead_workers(
    db: Session,
    async_db: AsyncSession,
    worker_service: WorkerService,
    worker_factory: WorkerFactory,
):
    now = await async_db.scalar(select(func.now()))
    dead_worker0 = worker_factory(last_heartbeat=now - datetime.timedelta(seconds=6))
    dead_worker1 = worker_factory(last_heartbeat=now - datetime.timedelta(seconds=7))
    alive_worker0 = worker_factory(last_heartbeat=now - datetime.timedelta(seconds=4))
    alive_worker1 = worker_factory(last_heartbeat=now - datetime.timedelta(seconds=3))
    alive_worker2 = worker_factory(last_heartbeat=now)
    dead_workers = await worker_service.fetch_dead_workers(5)
    await async_db.commit()
    db.expire_all()
    assert len(dead_workers) == 2
    assert frozenset(worker.id for worker in dead_workers) == frozenset(
        [dead_worker0.id, dead_worker1.id]
    )
    assert db.get(models.Worker, dead_worker0.id).state == models.WorkerState.NO_HEARTBEAT
    assert db.get(models.Worker, dead_worker1.id).state == models.WorkerState.NO_HEARTBEAT
    assert alive_worker0.state == models.WorkerState.RUNNING
    assert alive_worker1.state == models.WorkerState.RUNNING
    assert alive_worker2.state == models.WorkerState.RUNNING


async def test_reschedule_dead_tasks(
    db: Session,
    async_db: AsyncSession,
    worker_service: WorkerService,
    worker_factory: WorkerFactory,
    task_factory: TaskFactory,
):
    now = await async_db.scalar(select(func.now()))

    dead_worker0 = worker_factory(last_heartbeat=now - datetime.timedelta(seconds=6))
    dead_task0 = task_factory(worker=dead_worker0, state=models.TaskState.PROCESSING)
    dead_task1 = task_factory(worker=dead_worker0, state=models.TaskState.PROCESSING)
    done_task0 = task_factory(worker=dead_worker0, state=models.TaskState.DONE)

    dead_worker1 = worker_factory(last_heartbeat=now - datetime.timedelta(seconds=7))
    dead_task2 = task_factory(worker=dead_worker1, state=models.TaskState.PROCESSING)

    alive_worker0 = worker_factory(last_heartbeat=now - datetime.timedelta(seconds=4))
    other_task0 = task_factory(worker=alive_worker0, state=models.TaskState.PROCESSING)
    alive_worker1 = worker_factory()
    other_task1 = task_factory(worker=alive_worker1, state=models.TaskState.PROCESSING)

    task_count = await worker_service.reschedule_dead_tasks(
        [dead_worker0.id, dead_worker1.id]
    )
    await async_db.commit()
    db.expire_all()
    assert task_count == 3
    assert db.get(models.Task, dead_task0.id).state == models.TaskState.PENDING
    assert db.get(models.Task, dead_task0.id).worker is None
    assert db.get(models.Task, dead_task1.id).state == models.TaskState.PENDING
    assert db.get(models.Task, dead_task1.id).worker is None
    assert db.get(models.Task, dead_task2.id).state == models.TaskState.PENDING
    assert db.get(models.Task, dead_task2.id).worker is None
    assert db.get(models.Task, done_task0.id).state == models.TaskState.DONE
    assert db.get(models.Task, other_task0.id).state == models.TaskState.PROCESSING
    assert db.get(models.Task, other_task1.id).state == models.TaskState.PROCESSING
