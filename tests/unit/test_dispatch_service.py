import pytest
from sqlalchemy.orm import Session

from ..factories import TaskFactory
from bq import models
from bq.services.dispatch import DispatchService


@pytest.fixture
def dispatch_service() -> DispatchService:
    return DispatchService()


def test_dispatch_empty(
    db: Session, dispatch_service: DispatchService, worker: models.Worker
):
    assert not list(
        dispatch_service.dispatch(models.Task.channel == "test", worker=worker)
    )


def test_dispatch(
    db: Session,
    dispatch_service: DispatchService,
    worker: models.Worker,
    task: models.Task,
):
    assert task.state == models.TaskState.PENDING
    tasks = list(
        dispatch_service.dispatch(models.Task.channel == task.channel, worker=worker)
    )
    db.expire_all()
    assert len(tasks) == 1
    returned_task = tasks[0]
    assert returned_task.state == models.TaskState.PROCESSING
    assert returned_task.worker == worker
    assert not list(
        dispatch_service.dispatch(models.Task.channel == task.channel, worker=worker)
    )


def test_dispatch_many(
    db: Session,
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

    tasks = list(
        dispatch_service.dispatch(
            models.Task.channel == channel, worker=worker, limit=3
        )
    )
    db.expire_all()
    assert len(tasks) == 3
    for task in tasks:
        assert task.state == models.TaskState.PROCESSING
        assert task.worker == worker

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

    tasks = list(
        dispatch_service.dispatch(models.Task.channel == "my_channel", worker=worker)
    )
    assert len(tasks) == 1
