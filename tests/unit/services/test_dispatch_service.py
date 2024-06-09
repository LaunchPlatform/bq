import datetime

import pytest
from sqlalchemy import func
from sqlalchemy.orm import Session

from ...factories import TaskFactory
from bq import models
from bq.services.dispatch import DispatchService


@pytest.fixture
def dispatch_service(db: Session) -> DispatchService:
    return DispatchService(db)


def test_dispatch_empty(
    db: Session, dispatch_service: DispatchService, worker: models.Worker
):
    assert not list(dispatch_service.dispatch(["test"], worker_id=worker.id))


def test_dispatch(
    db: Session,
    dispatch_service: DispatchService,
    worker: models.Worker,
    task: models.Task,
):
    assert task.state == models.TaskState.PENDING
    tasks = list(dispatch_service.dispatch([task.channel], worker_id=worker.id))
    db.expire_all()
    assert len(tasks) == 1
    returned_task = tasks[0]
    assert returned_task.state == models.TaskState.PROCESSING
    assert returned_task.worker == worker
    assert not list(dispatch_service.dispatch([task.channel], worker_id=worker.id))


@pytest.mark.parametrize(
    "task__scheduled_at", [func.now() + datetime.timedelta(seconds=10)]
)
def test_dispatch_with_scheduled_at(
    db: Session,
    dispatch_service: DispatchService,
    worker: models.Worker,
    task: models.Task,
):
    assert task.state == models.TaskState.PENDING
    assert task.scheduled_at is not None

    tasks = list(dispatch_service.dispatch([task.channel], worker_id=worker.id))
    db.expire_all()
    assert len(tasks) == 0

    tasks = list(
        dispatch_service.dispatch(
            [task.channel],
            worker_id=worker.id,
            now=func.now() + datetime.timedelta(seconds=10),
        )
    )
    db.expire_all()
    assert len(tasks) == 1
    returned_task = tasks[0]
    assert returned_task.state == models.TaskState.PROCESSING
    assert returned_task.worker == worker


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

    tasks = list(dispatch_service.dispatch([channel], worker_id=worker.id, limit=3))
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

    tasks = list(dispatch_service.dispatch(["my_channel"], worker_id=worker.id))
    assert len(tasks) == 1


def test_listen_value_quote(db: Session, dispatch_service: DispatchService):
    dispatch_service.listen(["a", "中文", "!@#$%^&*(()-_"])
    db.commit()


def test_poll(db: Session, dispatch_service: DispatchService):
    dispatch_service.listen(["a", "b", "c"])
    db.commit()
    with pytest.raises(TimeoutError):
        list(dispatch_service.poll(timeout=1))
    dispatch_service.notify(["a", "c"])
    db.commit()
    notifications = list(dispatch_service.poll(timeout=1))
    assert frozenset([n.channel for n in notifications]) == frozenset(["a", "c"])
